use std::alloc::Layout;
use std::sync::Arc;
use connectorx::{
    partition::{partition, PartitionQuery},
    source_router::parse_source,
    sql::CXQuery,
};
use connectorx::source_router::{SourceConn, SourceType};
use connectorx::sources::oracle::OracleSource;
use connectorx::{
    prelude::*,
    sources::{
        mysql::{BinaryProtocol as MySQLBinaryProtocol, TextProtocol},
        postgres::{
            rewrite_tls_args, BinaryProtocol as PgBinaryProtocol, CSVProtocol, CursorProtocol,
            SimpleProtocol,
        },
    },
    sql::CXQuery,
};
use fehler::throw;
use pyo3::prelude::*;
use pyo3::{exceptions::PyValueError, PyResult};

use crate::errors::ConnectorXPythonError;

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub struct PyPartitionQuery {
    pub query: String,
    pub column: String,
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub num: usize,
}

impl Into<PartitionQuery> for PyPartitionQuery {
    fn into(self) -> PartitionQuery {
        PartitionQuery::new(
            self.query.as_str(),
            self.column.as_str(),
            self.min,
            self.max,
            self.num,
        )
    }
}

//用于从数据库中读取 SQL 查询结果，并将其转换为指定的返回类型（如 Pandas 或 Arrow）。该函数使用了 Rust 的异步编程特性以及 PyO3 库来与 Python 交互。
//生命周期 'py：表示该函数与 Python 解释器的生命周期相关联.
pub fn read_sql<'py>(
    py: Python<'py>, //py: Python<'py> 类型，表示 Python 解释器的引用.
    conn: &str,
    return_type: &str,
    protocol: Option<&str>,
    queries: Option<Vec<String>>,
    partition_query: Option<PyPartitionQuery>,
) -> PyResult<Bound<'py, PyAny>> {
    //使用 parse_source 函数解析数据库连接字符串和协议，返回一个 SourceConn 对象. 如果解析失败，则转换错误为 ConnectorXPythonError 并返回.
    let source_conn = parse_source(conn, protocol).map_err(|e| ConnectorXPythonError::from(e))?;

    let (queries, origin_query) = match (queries, partition_query) {
        (Some(queries), None) => (queries.into_iter().map(CXQuery::Naked).collect(), None),
        (None, Some(part)) => {
            let origin_query = Some(part.query.clone());
            //在这重写查询分区列与查询分区数
            let modified_part=rewriter_PyPartitionQuery(&source_conn,&mut part);

            let queries = partition(&modified_part.into(), &source_conn)
                .map_err(|e| ConnectorXPythonError::from(e))?;
            (queries, origin_query)
        }
        (Some(_), Some(_)) => throw!(PyValueError::new_err(
            "partition_query and queries cannot be both specified",
        )),
        (None, None) => throw!(PyValueError::new_err(
            "partition_query and queries cannot be both None",
        )),
    };

    match return_type {
        "pandas" => Ok(crate::pandas::write_pandas(
            py,
            &source_conn,
            origin_query,
            &queries,
        )?),
        "arrow" => Ok(crate::arrow::write_arrow(
            py,
            &source_conn,
            origin_query,
            &queries,
        )?),
        "arrow2" => Ok(crate::arrow2::write_arrow(
            py,
            &source_conn,
            origin_query,
            &queries,
        )?),
        _ => Err(PyValueError::new_err(format!(
            "return type should be 'pandas' or 'arrow', got '{}'",
            return_type
        ))),
    }
}
pub fn rewriter_PyPartitionQuery(
    source_coon:&SourceConn,
    part : &mut Option<PyPartitionQuery>,
)->PyPartitionQuery {
    //根据source——type与protocol定义Source对象
    match source_conn.ty {
        SourceType::Postgres| SourceType::cockroach => {
            let (config, tls) = rewrite_tls_args(&source_conn.conn)?;
            let sb= match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    PostgresSource::<CSVProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                    )?;
                }
                ("csv", None) => {
                        PostgresSource::<CSVProtocol, NoTls>::new(config, NoTls)?;
                }
                ("binary", Some(tls_conn)) => {
                    PostgresSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                    )?;
                }
                ("binary", None) => {
                    PostgresSource::<PgBinaryProtocol, NoTls>::new(
                        config,
                        NoTls,
                    )?;
                }
                ("cursor", Some(tls_conn)) => {
                    PostgresSource::<CursorProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                    )?;
                }
                ("cursor", None) => {
                    PostgresSource::<CursorProtocol, NoTls>::new(config, NoTls)?;
                }
                ("simple", Some(tls_conn)) => {
                    PostgresSource::<SimpleProtocol, MakeTlsConnector>::new(
                        config,
                        tls_conn,
                    )?;
                }
                ("simple", None) => {
                    PostgresSource::<SimpleProtocol, NoTls>::new(config, NoTls)?;
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            };
            sb.set_queries(part.query.as_str());
            sb.fetch_metadata();
            part.num=sb.available_threads;
            part.column=match(part.column==sb.index_name.clone()){
                false=>{
                    debug!("COLUMN MODIFY!!!!!");
                    part.min=None;
                    part.max=None;
                    part.column=sc.index_name.clone();
                }
                _=>{
                    debug!("COLUMN NO MODIFY!!!!!!!!!!!!!!!!!!!!");

                }
            };
            part;

        }
        SourceType::SQLite => {
            // remove the first "sqlite://" manually since url.path is not correct for windows
            let path = &source_conn.conn.as_str()[9..];
            SQLiteSource::new(path)?;
            part;
        }
        SourceType::MySQL => match protocol {
            "binary" => {
                MySQLSource::<MySQLBinaryProtocol>::new(&source_conn.conn[..])?;
            }
            "text" => {
                MySQLSource::<TextProtocol>::new(&source_conn.conn[..])?;
            }
            _ => unimplemented!("{} protocol not supported", protocol),
        },
        SourceType::MsSQL => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            MsSQLSource::new(rt, &source_conn.conn[..])?;
        }
        SourceType::Oracle => {
            OracleSource::new(&source_conn.conn[..])?;
            part;
        }
        SourceType::BigQuery => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            BigQuerySource::new(rt, &source_conn.conn[..])?;
            part;
        }
        SourceType::Trino => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            TrinoSource::new(rt, &source_conn.conn[..])?;
            part;
        }
        _ => unimplemented!("{:?} not implemented!", source_conn.ty),
    }

}