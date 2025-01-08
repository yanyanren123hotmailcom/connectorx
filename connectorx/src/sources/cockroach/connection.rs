use crate::sources::cockroach::errors::CockroachSourceError;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres::{config::SslMode, Config};
use postgres_openssl::MakeTlsConnector;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::PathBuf;
use url::Url;

//这个属性宏为 TlsConfig 结构体自动派生 Clone 和 Debug trait，这意味着你可以克隆这个结构体的实例，并且可以使用 {:?} 格式化宏来打印调试信息。
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// Postgres config, pg_config.sslmode (`sslmode`).
    pub pg_config: Config,
    /// Location of the client cert and key (`sslcert`, `sslkey`).
    pub client_cert: Option<(PathBuf, PathBuf)>,
    /// Location of the root certificate (`sslrootcert`).
    pub root_cert: Option<PathBuf>,
}


//一个 TryFrom<TlsConfig> 特型（trait），用于将 TlsConfig 转换为 MakeTlsConnector 类型，这是 postgres 库中用于创建 TLS 连接的类型。
//这段代码提供了一个灵活的方式来配置 TLS 连接，并根据 PostgreSQL 的 sslmode 设置来调整 TLS 连接的行为。
// 通过实现 TryFrom<TlsConfig> 特型，可以方便地将 TlsConfig 配置转换为 MakeTlsConnector 实例，用于与 PostgreSQL 数据库建立安全的 TLS 连接。
impl TryFrom<TlsConfig> for MakeTlsConnector {
    type Error = CockroachSourceError;
    // The logic of this function adapted primarily from:
    // https://github.com/sfackler/rust-postgres/pull/774
    // We only support server side authentication (`sslrootcert`) for now
    //实现了从 TlsConfig 到 MakeTlsConnector 的转换逻辑。
    fn try_from(tls_config: TlsConfig) -> Result<Self, Self::Error> {
        //创建 SslConnector 构建器，并设置为客户端模式 (SslMethod::tls_client())。
        let mut builder = SslConnector::builder(SslMethod::tls_client())?;
        //根据 pg_config 中的 ssl_mode 确定是否需要验证 CA 和主机名。
        let ssl_mode = tls_config.pg_config.get_ssl_mode();
        let (verify_ca, verify_hostname) = match ssl_mode {
            SslMode::Disable | SslMode::Prefer => (false, false),
            SslMode::Require => match tls_config.root_cert {
                // If a root CA file exists, the behavior of sslmode=require will be the same as
                // that of verify-ca, meaning the server certificate is validated against the CA.
                //
                // For more details, check out the note about backwards compatibility in
                // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
                Some(_) => (true, false),
                None => (false, false),
            },
            // These two modes will not work until upstream rust-postgres supports parsing
            // them as part of the TLS config.
            //
            // SslMode::VerifyCa => (true, false),
            // SslMode::VerifyFull => (true, true),
            _ => panic!("unexpected sslmode {:?}", ssl_mode),
        };
        //如果提供了客户端证书和私钥，则设置到构建器中。
        if let Some((cert, key)) = tls_config.client_cert {
            builder.set_certificate_file(cert, SslFiletype::PEM)?;
            builder.set_private_key_file(key, SslFiletype::PEM)?;
        }
        //如果提供了根证书，则设置为 CA 文件。
        if let Some(root_cert) = tls_config.root_cert {
            builder.set_ca_file(root_cert)?;
        }
        //根据 ssl_mode 设置验证模式。如果不验证 CA，则设置为 SslVerifyMode::NONE。
        if !verify_ca {
            builder.set_verify(SslVerifyMode::NONE); // do not verify CA
        }
        //创建 MakeTlsConnector，并根据是否验证主机名设置回调函数。
        let mut tls_connector = MakeTlsConnector::new(builder.build());

        if !verify_hostname {
            tls_connector.set_callback(|connect, _| {
                connect.set_verify_hostname(false);
                Ok(())
            });
        }

        Ok(tls_connector)
    }
}

// Strip URL params not accepted by upstream rust-postgres
//从一个 URL 中移除不被 rust-postgres 库接受的查询参数。这个函数接受一个 Url 类型的引用作为参数，并返回一个新的 Url 实例，其中已经去除了特定的查询参数。
fn strip_bad_opts(url: &Url) -> Url {
    let stripped_query: Vec<(_, _)> = url
        .query_pairs() //query_pairs(): 这个方法将 URL 的查询部分（query string）解析成一个迭代器，每个元素都是一个 (&str, &str) 对，代表一个键值对。
        .filter(|p| match &*p.0 {
            "sslkey" | "sslcert" | "sslrootcert" => false,
            _ => true,
        })  //迭代器的 filter 方法用于过滤掉不需要的查询参数。在这个例子中，被过滤掉的参数是 "sslkey"、"sslcert" 和 "sslrootcert"。
        .collect();//将过滤后的迭代器收集成一个 Vec<(_, _)> 向量。

    let mut url2 = url.clone();
    url2.set_query(None);// 将克隆后的 URL 的查询部分设置为 None，即移除所有查询参数。

    for pair in stripped_query {
        url2.query_pairs_mut()// 获取一个可变的查询参数迭代器。
            .append_pair(&pair.0.to_string()[..], &pair.1.to_string()[..]);//遍历 stripped_query 向量，并将每个键值对添加回 URL 的查询部分。
    }

    url2
}

//该函数用于处理 PostgreSQL 连接 URL，提取 TLS 相关的参数，并构建 Config 和 MakeTlsConnector 对象。
pub fn rewrite_tls_args(
    conn: &Url,
) -> Result<(Config, Option<MakeTlsConnector>), CockroachSourceError> {
    // We parse the config, then strip unsupported SSL opts and rewrite the URI
    // before calling conn.parse().
    //
    // For more details on this approach, see the conversation here:
    // https://github.com/sfackler/rust-postgres/pull/774#discussion_r641784774

    //使用 conn.query_pairs().into_owned().collect() 将 URL 的查询参数解析为 HashMap<String, String>。
    let params: HashMap<String, String> = conn.query_pairs().into_owned().collect();
    //从 HashMap 中提取 sslcert、sslkey 和 sslrootcert 参数，并转换为 PathBuf 类型。
    let sslcert = params.get("sslcert").map(PathBuf::from);
    let sslkey = params.get("sslkey").map(PathBuf::from);
    let root_cert = params.get("sslrootcert").map(PathBuf::from);
    let client_cert = match (sslcert, sslkey) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    };
    //使用 strip_bad_opts 函数去除连接 URL 中不被 rust-postgres 支持的 SSL 参数。
    let stripped_url = strip_bad_opts(conn);
    //将去除了 SSL 参数的 URL 字符串解析为 Config 对象。
    let pg_config: Config = stripped_url.as_str().parse().unwrap();

    let tls_config = TlsConfig {
        pg_config: pg_config.clone(),
        client_cert,
        root_cert,
    };

    let tls_connector = match pg_config.get_ssl_mode() {
        SslMode::Disable => None,
        _ => Some(MakeTlsConnector::try_from(tls_config)?),
    };

    Ok((pg_config, tls_connector))
}
