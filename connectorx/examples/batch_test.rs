use connectorx::arrow_batch_iter::ArrowBatchIter;
use connectorx::prelude::*;
use connectorx::sources::postgres::{rewrite_tls_args, BinaryProtocol as PgBinaryProtocol};
use postgres::NoTls;
use std::convert::TryFrom;
use std::time::Instant;

fn main() {
    // let queries = &[CXQuery::naked("select * from test_table")];
    // let queries = &[
    //     CXQuery::naked("select * from test_table where test_int < 3"),
    //     CXQuery::naked("select * from test_table where test_int >= 3"),
    // ];

    //记录开始时间
    let start = Instant::now();

    let queries = &[
        CXQuery::naked("select * from lineitem where l_orderkey < 1000000"),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 1000000 AND l_orderkey < 2000000",
        ),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 2000000 AND l_orderkey < 3000000",
        ),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 3000000 AND l_orderkey < 4000000",
        ),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 4000000 AND l_orderkey < 5000000",
        ),
        CXQuery::naked("select * from lineitem where l_orderkey >= 5000000"),
    ];

    let origin_query = None;

    let conn = "postgresql://postgres:postgres@localhost:5432/tpch";
    //定义了一个 PostgreSQL 连接字符串，并使用 SourceConn::try_from 来创建一个 SourceConn 实例
    let source = SourceConn::try_from(conn).unwrap();

    //使用 rewrite_tls_args 函数来处理 TLS 参数（在这个例子中没有使用 TLS，所以是 NoTls）
    let (config, _) = rewrite_tls_args(&source.conn).unwrap();
    //创建一个 PostgresSource 实例，它将用于从 PostgreSQL 数据库中读取数据。
    let source =
        PostgresSource::<PgBinaryProtocol, NoTls>::new(config, NoTls, queries.len()).unwrap();
    //创建一个 ArrowStreamDestination 实例，用于指定 Arrow 批次的目标。
    let destination = ArrowStreamDestination::new_with_batch_size(2048);
    //创建一个 ArrowBatchIter 实例，它将用于迭代从数据库中检索的数据。
    let mut batch_iter: ArrowBatchIter<_, PostgresArrowStreamTransport<PgBinaryProtocol, NoTls>> =
        ArrowBatchIter::new(source, destination, origin_query, queries).unwrap();
    //调用 batch_iter.prepare() 来准备批次迭代器。
    // 使用 for 循环来迭代 ArrowBatchIter 中的每个 record_batch，并打印每个批次的行数。
    // 累加总行数和批次数。
    batch_iter.prepare();

    let mut num_rows = 0;
    let mut num_batches = 0;
    for record_batch in batch_iter {
        let record_batch = record_batch;
        println!("got 1 batch, with {} rows", record_batch.num_rows());
        num_rows += record_batch.num_rows();
        num_batches += 1;
        // arrow::util::pretty::print_batches(&[record_batch]).unwrap();
    }
    println!(
        "got {} batches, {} rows in total, took {:?}",
        num_batches,
        num_rows,
        start.elapsed()
    );
}
