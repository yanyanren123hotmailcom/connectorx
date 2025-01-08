use crate::{prelude::*, sql::CXQuery};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use fehler::throws;
use log::debug;
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{mpsc::channel, Arc};

//它接受一个 SQL 查询字符串、一个数据库连接映射表、一个可选的 Java 4 Rust (j4rs) 基础路径和一个策略字符串作为参数，
// 并返回一个 RecordBatch 向量。这个函数的主要功能是执行一个联邦查询，即在多个数据源上执行 SQL 查询并将结果合并。
#[throws(ConnectorXOutError)]
pub fn run(
    sql: String,
    db_map: HashMap<String, String>,
    j4rs_base: Option<&str>,
    strategy: &str,
) -> Vec<RecordBatch> {
    debug!("federated input sql: {}", sql);
    let mut db_conn_map: HashMap<String, FederatedDataSourceInfo> = HashMap::new();
    for (k, v) in db_map.into_iter() {
        db_conn_map.insert(
            k,
            FederatedDataSourceInfo::new_from_conn_str(
                SourceConn::try_from(v.as_str())?,
                false,
                "",
                "",
            ),
        );
    }
    //使用 rewrite_sql 函数将输入的 SQL 查询重写为多个子查询，这些子查询可以在不同的数据源上执行。
    let fed_plan = rewrite_sql(sql.as_str(), &db_conn_map, j4rs_base, strategy)?;

    //使用 rayon 库的 into_par_iter 方法来并发执行每个子查询。
    // 对于本地查询（标记为 "LOCAL"），直接发送 SQL 语句。
    // 对于远程查询，将 SQL 语句分割并使用 get_arrow 函数获取结果，然后将结果注册为内存表（MemTable）.
    debug!("fetch queries from remote");
    let (sender, receiver) = channel();
    //使用 into_par_iter 方法将 fed_plan 向量转换为并行迭代器，以便并行处理每个子查询计划.
    //使用 enumerate 方法获取每个子查询计划的索引 (i) 和计划本身 (p)。
    //使用 try_for_each_with 方法来处理每个子查询计划，并返回 Result<(), ConnectorXOutError>，以便在发生错误时能够正确地传播错误.
    fed_plan.into_par_iter().enumerate().try_for_each_with(
        sender,
        |s, (i, p)| -> Result<(), ConnectorXOutError> {
            match p.db_name.as_str() {
                "LOCAL" => {
                    s.send((p.sql, None)).expect("send error local");
                }
                _ => {
                    debug!("start query {}: {}", i, p.sql);
                    let mut queries = vec![];
                    p.sql.split(';').for_each(|ss| {
                        queries.push(CXQuery::naked(ss));
                    });
                    //从 db_conn_map 中获取与子查询计划对应的数据库连接信息 (source_conn)。
                    let source_conn = &db_conn_map[p.db_name.as_str()]
                        .conn_str_info
                        .as_ref()
                        .unwrap();
                    //使用 get_arrow 函数执行查询并将结果转换为 Arrow 格式 (rbs)
                    let destination = get_arrow(source_conn, None, queries.as_slice())?;
                    let rbs = destination.arrow()?;

                    //创建一个 MemTable 实例，将查询结果作为数据源注册到内存表中.
                    let provider = MemTable::try_new(rbs[0].schema(), vec![rbs])?;
                    //将内存表的别名 (p.db_alias) 和内存表实例 (provider) 作为元组发送到通道中，表示这是一个远程查询的结果.
                    s.send((p.db_alias, Some(Arc::new(provider))))
                        .expect(&format!("send error {}", i));
                    debug!("query {} finished", i);
                }
            }
            Ok(())
        },
    )?;

    //注册表和处理本地查询：
    //
    // 使用 SessionContext 来注册每个子查询的结果表。
    // 处理本地 SQL 语句，如果存在，则将其存储在 local_sql 变量中。
    //参数
    // receiver: 一个通道的接收端，用于接收从发送端发送过来的查询结果.
    // ctx: SessionContext 实例，用于注册表和执行查询.
    // alias_names: 一个向量，用于存储注册表的别名.
    // local_sql: 一个字符串变量，用于存储本地查询的 SQL 语句.
    let ctx = SessionContext::new();
    let mut alias_names: Vec<String> = vec![];
    let mut local_sql = String::new();
    receiver
        .iter()
        .try_for_each(|(alias, provider)| -> Result<(), ConnectorXOutError> {
            match provider {
                //远程查询结果：
                // 如果 provider 是 Some(p)，表示这是一个远程查询的结果.
                // 使用 ctx.register_table 方法将结果注册为表，表名为 alias，表数据为 p（即 MemTable 实例）.
                // 将表的别名 alias 添加到 alias_names 向量中，以便后续使用.
                Some(p) => {
                    ctx.register_table(alias.as_str(), p)?;
                    alias_names.push(alias);
                }
                //本地查询：
                // 如果 provider 是 None，表示这是一个本地查询的结果.
                // 将 alias 赋值给 local_sql，表示这是本地查询的 SQL 语句.
                None => local_sql = alias,
            }

            Ok(())
        })?;

    debug!("\nexecute query final...\n{}\n", local_sql);
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
    // until datafusion fix the bug: https://github.com/apache/arrow-datafusion/issues/2147
    for alias in alias_names {
        //由于 DataFusion 存在一个已知的 Bug（如文件 1 中提到的），在执行最终查询之前，需要将所有带引号的表名替换为不带引号的表名.
        local_sql = local_sql.replace(format!("\"{}\"", alias).as_str(), alias.as_str());
    }
    //使用 ctx.sql(local_sql.as_str()) 执行最终的 SQL 查询，并将结果存储在 df 中。这里 ctx 是一个 SessionContext 实例，用于执行查询.
    // 使用 rt.block_on 方法来同步执行异步查询操作，确保查询能够完成并返回结果.
    let df = rt.block_on(ctx.sql(local_sql.as_str()))?;
    //收集查询结果：
    //
    // 使用 df.collect() 方法收集查询结果，并再次使用 rt.block_on 方法来同步执行异步收集操作，确保结果能够被正确收集.
    // 如果执行查询或收集结果时发生错误，会返回一个错误.
    rt.block_on(df.collect())?
}
