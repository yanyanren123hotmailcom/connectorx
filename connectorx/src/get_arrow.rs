#[cfg(feature = "src_mysql")]
use crate::sources::mysql::{BinaryProtocol as MySQLBinaryProtocol, TextProtocol};

#[cfg(feature = "src_postgres")]
use crate::sources::postgres::{
    rewrite_tls_args, BinaryProtocol as PgBinaryProtocol, CSVProtocol, CursorProtocol,
    SimpleProtocol,
};
use crate::{
    arrow_batch_iter::{ArrowBatchIter, RecordBatchIterator},
    prelude::*,
    sql::CXQuery,
};
use fehler::{throw, throws};
use log::debug;
#[cfg(feature = "src_postgres")]
use postgres::NoTls;
#[cfg(feature = "src_postgres")]
use postgres_openssl::MakeTlsConnector;
#[allow(unused_imports)]
use std::sync::Arc;
use crate::sources::cockroach::CockroachSource;

//主要功能是根据不同的数据源类型和协议，执行 SQL 查询并将结果转换为 Arrow 格式。这个函数使用了多个条件编译特性来支持不同的数据库类型和协议。
#[allow(unreachable_code, unreachable_patterns, unused_variables, unused_mut)]
#[throws(ConnectorXOutError)]
pub fn get_arrow(
    source_conn: &SourceConn,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
) -> ArrowDestination {
    //创建一个新的 ArrowDestination 实例，用于存储查询结果.
    let mut destination = ArrowDestination::new();
    let protocol = source_conn.proto.as_str();
    debug!("Protocol: {}", protocol);
    //根据数据源类型和协议执行查询：
    //
    // 使用 match 语句根据数据源类型 (source_conn.ty) 和协议类型来选择不同的处理方式.
    // 对于每种数据源类型（如 PostgreSQL、MySQL、SQLite、MsSQL、Oracle、BigQuery），根据协议类型创建相应的数据源实例（如 PostgresSource、MySQLSource 等）.
    // 创建一个 Dispatcher 实例，将数据源、目的地、查询和原始查询作为参数传递给 Dispatcher，并调用 run 方法来执行查询并将结果写入目的地.
    // 对于不同的协议和 TLS 配置，使用不同的数据源和传输方式（如 CSVProtocol、PgBinaryProtocol、MakeTlsConnector 等）.
    match source_conn.ty {
        #[cfg(feature = "src_postgres")]
        SourceType::Postgres => {
            let (config, tls) = rewrite_tls_args(&source_conn.conn)?;
            match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    let source = PostgresSource::<CSVProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CSVProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("csv", None) => {
                    let source =
                        PostgresSource::<CSVProtocol, NoTls>::new(config, NoTls, queries.len())?;
                    let dispatcher =
                        Dispatcher::<_, _, PostgresArrowTransport<CSVProtocol, NoTls>>::new(
                            source,
                            &mut destination,
                            queries,
                            origin_query,
                        );
                    dispatcher.run()?;
                }
                ("binary", Some(tls_conn)) => {
                    let source = PostgresSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<PgBinaryProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("binary", None) => {
                    let source = PostgresSource::<PgBinaryProtocol, NoTls>::new(
                        config,
                        NoTls,
                        queries.len(),
                    )?;
                    //对于不同的协议和 TLS 配置，使用不同的数据源和传输方式（如 CSVProtocol、PgBinaryProtocol、MakeTlsConnector 等）.
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<PgBinaryProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("cursor", Some(tls_conn)) => {
                    let source = PostgresSource::<CursorProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CursorProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("cursor", None) => {
                    let source =
                        PostgresSource::<CursorProtocol, NoTls>::new(config, NoTls, queries.len())?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CursorProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("simple", Some(tls_conn)) => {
                    let sb = PostgresSource::<SimpleProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<SimpleProtocol, MakeTlsConnector>,
                    >::new(
                        sb, &mut destination, queries, origin_query
                    );
                    debug!("Running dispatcher");
                    dispatcher.run()?;
                }
                ("simple", None) => {
                    let sb =
                        PostgresSource::<SimpleProtocol, NoTls>::new(config, NoTls, queries.len())?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<SimpleProtocol, NoTls>,
                    >::new(
                        sb, &mut destination, queries, origin_query
                    );
                    debug!("Running dispatcher");
                    dispatcher.run()?;
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        SourceType::Cockroach => {
            let (config, tls) = rewrite_tls_args(&source_conn.conn)?;
            match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    let source = CockroachSource::<CSVProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<CSVProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("csv", None) => {
                    let source =
                        CockroachSource::<CSVProtocol, NoTls>::new(config, NoTls, queries.len())?;
                    let dispatcher =
                        Dispatcher::<_, _, CockroachArrowTransport<CSVProtocol, NoTls>>::new(
                            source,
                            &mut destination,
                            queries,
                            origin_query,
                        );
                    dispatcher.run()?;
                }
                ("binary", Some(tls_conn)) => {
                    let source = CockroachSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<PgBinaryProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("binary", None) => {
                    let source = CockroachSource::<PgBinaryProtocol, NoTls>::new(
                        config,
                        NoTls,
                        queries.len(),
                    )?;
                    //对于不同的协议和 TLS 配置，使用不同的数据源和传输方式（如 CSVProtocol、PgBinaryProtocol、MakeTlsConnector 等）.
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<PgBinaryProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("cursor", Some(tls_conn)) => {
                    let source = CockroachSource::<CursorProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<CursorProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("cursor", None) => {
                    let source =
                        CockroachSource::<CursorProtocol, NoTls>::new(config, NoTls, queries.len())?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<CursorProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.run()?;
                }
                ("simple", Some(tls_conn)) => {
                    let sb = CockroachSource::<SimpleProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<SimpleProtocol, MakeTlsConnector>,
                    >::new(
                        sb, &mut destination, queries, origin_query
                    );
                    debug!("Running dispatcher");
                    dispatcher.run()?;
                }
                ("simple", None) => {
                    let sb =
                        CockroachSource::<SimpleProtocol, NoTls>::new(config, NoTls, queries.len())?;
                    let dispatcher = Dispatcher::<
                        _,
                        _,
                        CockroachArrowTransport<SimpleProtocol, NoTls>,
                    >::new(
                        sb, &mut destination, queries, origin_query
                    );
                    debug!("Running dispatcher");
                    dispatcher.run()?;
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_mysql")]
        SourceType::MySQL => match protocol {
            "binary" => {
                let source =
                    MySQLSource::<MySQLBinaryProtocol>::new(&source_conn.conn[..], queries.len())?;
                let dispatcher = Dispatcher::<_, _, MySQLArrowTransport<MySQLBinaryProtocol>>::new(
                    source,
                    &mut destination,
                    queries,
                    origin_query,
                );
                dispatcher.run()?;
            }
            "text" => {
                let source =
                    MySQLSource::<TextProtocol>::new(&source_conn.conn[..], queries.len())?;
                let dispatcher = Dispatcher::<_, _, MySQLArrowTransport<TextProtocol>>::new(
                    source,
                    &mut destination,
                    queries,
                    origin_query,
                );
                dispatcher.run()?;
            }
            _ => unimplemented!("{} protocol not supported", protocol),
        },
        #[cfg(feature = "src_sqlite")]
        SourceType::SQLite => {
            // remove the first "sqlite://" manually since url.path is not correct for windows
            let path = &source_conn.conn.as_str()[9..];
            let source = SQLiteSource::new(path, queries.len())?;
            let dispatcher = Dispatcher::<_, _, SQLiteArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_mssql")]
        SourceType::MsSQL => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = MsSQLSource::new(rt, &source_conn.conn[..], queries.len())?;
            let dispatcher = Dispatcher::<_, _, MsSQLArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_oracle")]
        SourceType::Oracle => {
            let source = OracleSource::new(&source_conn.conn[..], queries.len())?;
            let dispatcher = Dispatcher::<_, _, OracleArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_bigquery")]
        SourceType::BigQuery => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = BigQuerySource::new(rt, &source_conn.conn[..])?;
            let dispatcher = Dispatcher::<_, _, BigQueryArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        _ => throw!(ConnectorXOutError::SourceNotSupport(format!(
            "{:?}",
            source_conn.ty
        ))),
    }

    destination
}

//主要功能是根据不同的数据源类型和协议，创建一个 RecordBatchIterator。这个函数使用了条件编译和模式匹配来支持多种数据库类型和协议。
#[allow(unreachable_code, unreachable_patterns, unused_variables, unused_mut)]
pub fn new_record_batch_iter(
    source_conn: &SourceConn,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    batch_size: usize,
) -> Box<dyn RecordBatchIterator> {
    let destination = ArrowStreamDestination::new_with_batch_size(batch_size);
    let protocol = source_conn.proto.as_str();
    debug!("Protocol: {}", protocol);

    match source_conn.ty {
        #[cfg(feature = "src_postgres")]
        SourceType::Postgres => {
            let (config, tls) = rewrite_tls_args(&source_conn.conn).unwrap();
            //使用 match 语句根据数据源类型 (source_conn.ty) 和协议类型来选择不同的处理方式.
            // 对于每种数据源类型（如 PostgreSQL、MySQL、SQLite、MsSQL、Oracle、BigQuery），根据协议类型创建相应的数据源实例（如 PostgresSource、MySQLSource 等）.
            // 创建一个 ArrowBatchIter 实例，将数据源、目的地、查询和原始查询作为参数传递给 ArrowBatchIter，并返回一个 Box<dyn RecordBatchIterator> 类型的动态迭代器.
            // 对于不同的协议和 TLS 配置，使用不同的数据源和传输方式（如 CSVProtocol、PgBinaryProtocol、MakeTlsConnector 等）.
            match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    let source = PostgresSource::<CSVProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )
                    .unwrap();
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<CSVProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("csv", None) => {
                    let source =
                        PostgresSource::<CSVProtocol, NoTls>::new(config, NoTls, queries.len())
                            .unwrap();
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<CSVProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                    .unwrap();
                    return Box::new(batch_iter);
                }
                ("binary", Some(tls_conn)) => {
                    let source = PostgresSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )
                    .unwrap();
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<PgBinaryProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("binary", None) => {
                    let source = PostgresSource::<PgBinaryProtocol, NoTls>::new(
                        config,
                        NoTls,
                        queries.len(),
                    )
                    .unwrap();
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<PgBinaryProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                    .unwrap();
                    return Box::new(batch_iter);
                }
                ("cursor", Some(tls_conn)) => {
                    let source = PostgresSource::<CursorProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )
                    .unwrap();
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<CursorProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("cursor", None) => {
                    let source =
                        PostgresSource::<CursorProtocol, NoTls>::new(config, NoTls, queries.len())
                            .unwrap();
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<CursorProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                    .unwrap();
                    return Box::new(batch_iter);
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_cockroach")]
        SourceType::Cockroach=> {
            let (config, tls) = rewrite_tls_args(&source_conn.conn).unwrap();
            //使用 match 语句根据数据源类型 (source_conn.ty) 和协议类型来选择不同的处理方式.
            // 对于每种数据源类型（如 PostgreSQL、MySQL、SQLite、MsSQL、Oracle、BigQuery），根据协议类型创建相应的数据源实例（如 PostgresSource、MySQLSource 等）.
            // 创建一个 ArrowBatchIter 实例，将数据源、目的地、查询和原始查询作为参数传递给 ArrowBatchIter，并返回一个 Box<dyn RecordBatchIterator> 类型的动态迭代器.
            // 对于不同的协议和 TLS 配置，使用不同的数据源和传输方式（如 CSVProtocol、PgBinaryProtocol、MakeTlsConnector 等）.
            match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    let source = PostgresSource::<CSVProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )
                        .unwrap();
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<CSVProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                            .unwrap();
                    return Box::new(batch_iter);
                }
                ("csv", None) => {
                    let source =
                        PostgresSource::<CSVProtocol, NoTls>::new(config, NoTls, queries.len())
                            .unwrap();
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<CSVProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("binary", Some(tls_conn)) => {
                    let source = PostgresSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )
                        .unwrap();
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<PgBinaryProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                            .unwrap();
                    return Box::new(batch_iter);
                }
                ("binary", None) => {
                    let source = PostgresSource::<PgBinaryProtocol, NoTls>::new(
                        config,
                        NoTls,
                        queries.len(),
                    )
                        .unwrap();
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<PgBinaryProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("cursor", Some(tls_conn)) => {
                    let source = PostgresSource::<CursorProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                        queries.len(),
                    )
                        .unwrap();
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<CursorProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                            .unwrap();
                    return Box::new(batch_iter);
                }
                ("cursor", None) => {
                    let source =
                        PostgresSource::<CursorProtocol, NoTls>::new(config, NoTls, queries.len())
                            .unwrap();
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<CursorProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                        .unwrap();
                    return Box::new(batch_iter);
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_mysql")]
        SourceType::MySQL => match protocol {
            "binary" => {
                let source =
                    MySQLSource::<MySQLBinaryProtocol>::new(&source_conn.conn[..], queries.len())
                        .unwrap();
                let batch_iter =
                    ArrowBatchIter::<_, MySQLArrowStreamTransport<MySQLBinaryProtocol>>::new(
                        source,
                        destination,
                        origin_query,
                        queries,
                    )
                    .unwrap();
                return Box::new(batch_iter);
            }
            "text" => {
                let source =
                    MySQLSource::<TextProtocol>::new(&source_conn.conn[..], queries.len()).unwrap();
                let batch_iter = ArrowBatchIter::<_, MySQLArrowStreamTransport<TextProtocol>>::new(
                    source,
                    destination,
                    origin_query,
                    queries,
                )
                .unwrap();
                return Box::new(batch_iter);
            }
            _ => unimplemented!("{} protocol not supported", protocol),
        },
        #[cfg(feature = "src_sqlite")]
        SourceType::SQLite => {
            // remove the first "sqlite://" manually since url.path is not correct for windows
            let path = &source_conn.conn.as_str()[9..];
            let source = SQLiteSource::new(path, queries.len()).unwrap();
            let batch_iter = ArrowBatchIter::<_, SQLiteArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        #[cfg(feature = "src_mssql")]
        SourceType::MsSQL => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = MsSQLSource::new(rt, &source_conn.conn[..], queries.len()).unwrap();
            let batch_iter = ArrowBatchIter::<_, MsSQLArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        #[cfg(feature = "src_oracle")]
        SourceType::Oracle => {
            let source = OracleSource::new(&source_conn.conn[..], queries.len()).unwrap();
            let batch_iter = ArrowBatchIter::<_, OracleArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        #[cfg(feature = "src_bigquery")]
        SourceType::BigQuery => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = BigQuerySource::new(rt, &source_conn.conn[..]).unwrap();
            let batch_iter = ArrowBatchIter::<_, BigQueryArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        _ => {}
    }
    panic!("not supported!");
}
