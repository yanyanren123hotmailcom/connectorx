//! Source implementation for Postgres database, including the TLS support (client only).

mod connection;
mod errors;
mod typesystem;

pub use self::errors::CockroachSourceError;
pub use connection::rewrite_tls_args;
pub use typesystem::{CockroachTypePairs, CockroachTypeSystem};
use num_cpus;
use sysinfo;
use regex::Regex;
use std::thread;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, TableFactor};
use crate::constants::DB_BUFFER_SIZE;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourcePartition},
    sql::{count_query, CXQuery},
};
use anyhow::anyhow;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use csv::{ReaderBuilder, StringRecord, StringRecordsIntoIter};
use fehler::{throw, throws};
use hex::decode;
use postgres::{
    binary_copy::{BinaryCopyOutIter, BinaryCopyOutRow},
    fallible_iterator::FallibleIterator,
    tls::{MakeTlsConnect, TlsConnect},
    Config, CopyOutReader, Row, RowIter, SimpleQueryMessage, Socket,
};
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use rust_decimal::Decimal;
use serde_json::{from_str, Value};
use sqlparser::dialect::PostgreSqlDialect;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::marker::PhantomData;
use uuid::Uuid;
use std::error::Error;

/// Protocol - Binary based bulk load
pub enum BinaryProtocol {}

/// Protocol - CSV based bulk load
pub enum CSVProtocol {}

/// Protocol - use Cursor
pub enum CursorProtocol {}

/// Protocol - use Simple Query
pub enum SimpleProtocol {}

type PgManager<C> = PostgresConnectionManager<C>;
type PgConn<C> = PooledConnection<PgManager<C>>;

// take a row and unwrap the interior field from column 0：
// 从 PostgreSQL 数据库查询结果的单行中获取的值转换为指定的 Rust 类型 R
//尝试将 postgres 库中的 Row 对象转换为泛型类型 R。这个函数假设 R 可以实现 TryFrom<usize> 和 postgres::types::FromSql 特征（traits），并且可以克隆。
//'b: 生命周期注解，表示 row 参数和返回值中的 R 对象都绑定到相同的生命周期。
// R: 泛型参数，约束为实现 TryFrom<usize>、postgres::types::FromSql<'b> 和 Clone 特征的类型。
// row: 函数参数，类型为 &'b Row，表示一个对 Row 的不可变引用，这个 Row 对象包含了从数据库查询返回的结果。
// -> R: 函数返回类型为 R。
fn convert_row<'b, R: TryFrom<usize> + postgres::types::FromSql<'b> + Clone>(row: &'b Row) -> R {
    let nrows: Option<R> = row.get(0);
    nrows.expect("Could not parse int result from count_query")
}
//用于执行一个 SQL 查询并返回查询结果的行数。这个函数接受一个可变引用到 PgConn<C> 类型的数据库连接和一个 CXQuery<String> 类型的查询对象。
//泛型参数和约束：
//
// C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send：C 是一个泛型参数，它必须满足一系列 trait 约束，包括能够创建 TLS 连接、可克隆、静态生命周期、线程安全和可发送到其他线程。
// C::TlsConnect: Send：C 的 TlsConnect 关联类型必须是可发送的。
// C::Stream: Send：C 的 Stream 关联类型必须是可发送的。
// <C::TlsConnect as TlsConnect<Socket>>::Future: Send：C::TlsConnect 的 Future 必须是可发送的。
#[throws(CockroachSourceError)]
fn get_total_rows<C>(conn: &mut PgConn<C>, query: &CXQuery<String>) -> usize
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    //创建一个 PostgreSQL 方言实例
    let dialect = PostgreSqlDialect {};

    let row = conn.query_one(count_query(query, &dialect)?.as_str(), &[])?;
    let col_type = CockroachTypeSystem::from(row.columns()[0].type_());
    match col_type {
        CockroachTypeSystem::Int2(_) => convert_row::<i16>(&row) as usize,
        CockroachTypeSystem::Int4(_) => convert_row::<i32>(&row) as usize,
        CockroachTypeSystem::Int8(_) => convert_row::<i64>(&row) as usize,
        _ => throw!(anyhow!(
            "The result of the count query was not an int, aborting."
        )),
    }
}

fn get_index_names<C>(conn: &mut PgConn<C>, query: &CXQuery<String>) -> Result<Vec<String>, Box<dyn Error>>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // 使用正则表达式解析查询中的表名
    let table_name_re = Regex::new(r"\bFROM\b\s+([\w]+)")?;
    let table_name_caps = table_name_re.captures(query.as_str()).unwrap();
    if let Some(caps) = table_name_caps {
        let table_name = caps.get(1).ok_or("Unable to extract table name")?.as_str();

        // 构造 SHOW INDEX FROM ${table_name} 查询
        let index_query = format!("SHOW INDEX FROM {}", table_name);

        // 执行查询以获取索引信息
        let rows = conn.query(&index_query, &[])?;

        // 存储索引项的 column_name
        let mut index_names = Vec::new();
        for row in rows {
            let column_name: String = row.get("column_name");
            index_names.push(column_name);
        }

        Ok(index_names)
    } else {
        Err("Unable to extract table name".into())
    }
}

pub struct CockroachSource<P, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pool: Pool<PgManager<C>>,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<CockroachTypeSystem>,
    pg_schema: Vec<postgres::types::Type>,
    _protocol: PhantomData<P>,
    idle_threads: usize,//当前空闲线程数量
    index_name: Option<String>,
    available_threads: usize,
}

impl<P, C> CockroachSource<P, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[throws(CockroachSourceError)]
    pub fn new(config: Config, tls: C, nconn: usize) -> Self {
        let manager = PostgresConnectionManager::new(config, tls);
        let pool = Pool::builder().max_size(nconn as u32).build(manager)?;

        Self {
            pool,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            pg_schema: vec![],
            _protocol: PhantomData,
            idle_threads:0,
            index_name: None,
            available_threads: 0,
        }
    }
}

impl<P, C> Source for CockroachSource<P, C>
where
    CockroachSourcePartition<P, C>:
        SourcePartition<TypeSystem = CockroachTypeSystem, Error = CockroachSourceError>,
    P: Send,
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = CockroachSourcePartition<P, C>;
    type TypeSystem = CockroachTypeSystem;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }
    }

    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    #[throws(CockroachSourceError)]
    fn fetch_metadata(&mut self) {
        assert!(!self.queries.is_empty());

        let mut conn = self.pool.get()?;
        let first_query = &self.queries[0];

        let stmt = conn.prepare(first_query.as_str())?;
        //获取查询结果的列名和类型
        let (names, pg_types): (Vec<String>, Vec<postgres::types::Type>) = stmt
            .columns()
            .iter()
            .map(|col| (col.name().to_string(), col.type_().clone()))
            .unzip();

        self.names = names;
        // 将 PostgreSQL 的类型转换为 CockroachTypeSystem 类型，并存储在 self.schema 中。
        self.schema = pg_types.iter().map(CockroachTypeSystem::from).collect();
        self.pg_schema = self
            .schema
            .iter()
            .zip(pg_types.iter())
            .map(|(t1, t2)| CockroachTypePairs(t2, t1).into())
            .collect();

        // 获取可用的处理器核心数
        let available_parallelism = thread::available_parallelism().unwrap();
        self.available_threads = available_parallelism.get();

        //获取索引信息
        let dialect = PostgreSqlDialect {};
        //提取表名
        let mut table_name=vec![];
        match Parser::parse_sql(&dialect,first_query.as_str() ) {
            Ok(statements) => {
                for statement in statements {
                    match statement {
                        Statement::Query(query) => {
                            if let Some(from) = &query.body {
                                if let sqlparser::ast::SetExpr::Select(select) = from {
                                    if let Some(table_with_joins) = &select.from {
                                        for table_with_join in table_with_joins {
                                            if let TableFactor::Table { name, .. } = &table_with_join.relation {
                                                table_name.push(name.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("Parse error: {}", e);
            }
        }

        //获取表的索引列信息
        // 使用 SHOW COLUMNS 语句获取列信息
        let index_info_query = format!("SHOW COLUMNS FROM {}", table_name[0]);
        let index_rows = conn.query(&index_info_query, &[])?;

        // 遍历结果并打印索引列信息

        //获取所有主键列
        let mut index_names=vec![];
        for row in index_rows {
            if row.get("indices")=="primary" {
                index_names.push(row.get("column_name"))
            }
        }
        //// 使用 SHOW INDEX 语句获取索引信息
        let index_info_query = format!("SHOW INDEX FROM {}", table_name[0]);
        let rows = conn.query(&index_info_query, &[])?;

        for row in rows {
            if row.get("seq_in_index")==1{
                if  index_names.contains(&row.get("column_name")) {
                    self.index_name=Some(row.get("column_name"))
                }
            }
        }
    }

    #[throws(CockroachSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => {
                let cxq = CXQuery::Naked(q.clone());
                let mut conn = self.pool.get()?;
                let nrows = get_total_rows(&mut conn, &cxq)?;
                Some(nrows)
            }
            None => None,
        }
    }

    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.schema.clone()
    }

    #[throws(CockroachSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        let mut ret = vec![];
        for query in self.queries {
            let conn = self.pool.get()?;

            ret.push(CockroachSourcePartition::<P, C>::new(
                conn,
                &query,
                &self.schema,
                &self.pg_schema,
            ));
        }
        ret
    }
}

pub struct CockroachSourcePartition<P, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn: PgConn<C>,
    query: CXQuery<String>,
    schema: Vec<CockroachTypeSystem>,
    pg_schema: Vec<postgres::types::Type>,
    nrows: usize,
    ncols: usize,
    _protocol: PhantomData<P>,
}

impl<P, C> CockroachSourcePartition<P, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(
        conn: PgConn<C>,
        query: &CXQuery<String>,
        schema: &[CockroachTypeSystem],
        pg_schema: &[postgres::types::Type],
    ) -> Self {
        Self {
            conn,
            query: query.clone(),
            schema: schema.to_vec(),
            pg_schema: pg_schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
            _protocol: PhantomData,
        }
    }
}

impl<C> SourcePartition for CockroachSourcePartition<BinaryProtocol, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type TypeSystem = CockroachTypeSystem;
    type Parser<'a> = CockroachBinarySourcePartitionParser<'a>;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn result_rows(&mut self) -> () {
        self.nrows = get_total_rows(&mut self.conn, &self.query)?;
    }

    //parser 的方法，该方法用于创建一个解析器，用于解析从 PostgreSQL 数据库复制出来的二进制数据流。
    // 这个方法是 #[throws(CockroachSourceError)] 属性的，意味着它可能会抛出 CockroachSourceError 类型的错误
    #[throws(CockroachSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        //创建了一个 SQL 查询字符串，用于复制指定的 SQL 查询结果到标准输出，并以二进制格式传输。
        let query = format!("COPY ({}) TO STDOUT WITH BINARY", self.query);
        //使用 copy_out 方法执行格式化后的 SQL 查询，该方法返回一个 CopyOut 读取器，用于读取从数据库复制出来的数据。? 操作符用于传播错误。
        let reader = self.conn.copy_out(&*query)?; // unless reading the data, it seems like issue the query is fast
        //创建了一个 BinaryCopyOutIter 迭代器，它将用于迭代从 reader 中读取的二进制数据流。迭代器的构造函数接受 reader 和 pg_schema 作为参数，其中 pg_schema 包含了 PostgreSQL 数据类型的信息。
        let iter = BinaryCopyOutIter::new(reader, &self.pg_schema);
        //创建了一个 CockroachBinarySourcePartitionParser 解析器，它将用于解析迭代器中的二进制数据，并将其转换为 Rust 数据结构。解析器的构造函数接受迭代器和 schema 作为参数。
        CockroachBinarySourcePartitionParser::new(iter, &self.schema)
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

impl<C> SourcePartition for CockroachSourcePartition<CSVProtocol, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type TypeSystem = CockroachTypeSystem;
    type Parser<'a> = CockroachCSVSourceParser<'a>;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn result_rows(&mut self) {
        self.nrows = get_total_rows(&mut self.conn, &self.query)?;
    }

    #[throws(CockroachSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let query = format!("COPY ({}) TO STDOUT WITH CSV", self.query);
        let reader = self.conn.copy_out(&*query)?; // unless reading the data, it seems like issue the query is fast
        let iter = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(reader)
            .into_records();

        CockroachCSVSourceParser::new(iter, &self.schema)
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

impl<C> SourcePartition for CockroachSourcePartition<CursorProtocol, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type TypeSystem = CockroachTypeSystem;
    type Parser<'a> = CockroachRawSourceParser<'a>;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn result_rows(&mut self) {
        self.nrows = get_total_rows(&mut self.conn, &self.query)?;
    }

    #[throws(CockroachSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let iter = self
            .conn
            .query_raw::<_, bool, _>(self.query.as_str(), vec![])?; // unless reading the data, it seems like issue the query is fast
        CockroachRawSourceParser::new(iter, &self.schema)
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}
pub struct CockroachBinarySourcePartitionParser<'a> {
    iter: BinaryCopyOutIter<'a>,
    rowbuf: Vec<BinaryCopyOutRow>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> CockroachBinarySourcePartitionParser<'a> {
    pub fn new(iter: BinaryCopyOutIter<'a>, schema: &[CockroachTypeSystem]) -> Self {
        Self {
            iter,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(CockroachSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for CockroachBinarySourcePartitionParser<'a> {
    type TypeSystem = CockroachTypeSystem;
    type Error = CockroachSourceError;

    //返回一个元组，包含剩余行数和是否完成的标志。
    #[throws(CockroachSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        // clear the buffer
        if !self.rowbuf.is_empty() {
            self.rowbuf.drain(..);
        }
        for _ in 0..DB_BUFFER_SIZE {
            match self.iter.next()? {
                Some(row) => {
                    self.rowbuf.push(row);
                }
                None => {
                    self.is_finished = true;
                    break;
                }
            }
        }

        // reset current cursor positions
        self.current_row = 0;
        self.current_col = 0;

        (self.rowbuf.len(), self.is_finished)
    }
}

//定义了一个名为impl_produce的宏，它用于为CockroachBinarySourcePartitionParser结构体实现 Produce 特征，这个特征允许从解析器中生成（produce）特定类型的值。
// 宏接受一系列类型作为参数，并为每个类型生成两个 Produce 特征的实现：一个用于生成 T 类型的值，另一个用于生成 Option<T> 类型的值。
macro_rules! impl_produce {
    ($($t: ty,)+) => { //这是宏的模式，它匹配一系列以逗号分隔的类型。
        $(
            impl<'r, 'a> Produce<'r, $t> for CockroachBinarySourcePartitionParser<'a> { //实现 Produce 特征，允许生成类型为 $t 的值。
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let row = &self.rowbuf[ridx];
                    let val = row.try_get(cidx)?;
                    val
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for CockroachBinarySourcePartitionParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let row = &self.rowbuf[ridx];
                    let val = row.try_get(cidx)?;
                    val
                }
            }
        )+
    };
}

impl_produce!(
    i8,
    i16,
    i32,
    i64,
    f32,
    f64,
    Decimal,
    Vec<i16>,
    Vec<i32>,
    Vec<i64>,
    Vec<f32>,
    Vec<f64>,
    Vec<Decimal>,
    bool,
    Vec<bool>,
    &'r str,
    Vec<u8>,
    NaiveTime,
    // NaiveDateTime,
    // DateTime<Utc>,
    // NaiveDate,
    Uuid,
    Value,
    Vec<String>,
);

impl<'r, 'a> Produce<'r, NaiveDateTime> for CockroachBinarySourcePartitionParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveDateTime {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            postgres::types::Timestamp::PosInfinity => NaiveDateTime::MAX,
            postgres::types::Timestamp::NegInfinity => NaiveDateTime::MIN,
            postgres::types::Timestamp::Value(t) => t,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveDateTime>> for CockroachBinarySourcePartitionParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveDateTime> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            Some(postgres::types::Timestamp::PosInfinity) => Some(NaiveDateTime::MAX),
            Some(postgres::types::Timestamp::NegInfinity) => Some(NaiveDateTime::MIN),
            Some(postgres::types::Timestamp::Value(t)) => t,
            None => None,
        }
    }
}

impl<'r, 'a> Produce<'r, DateTime<Utc>> for CockroachBinarySourcePartitionParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> DateTime<Utc> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            postgres::types::Timestamp::PosInfinity => DateTime::<Utc>::MAX_UTC,
            postgres::types::Timestamp::NegInfinity => DateTime::<Utc>::MIN_UTC,
            postgres::types::Timestamp::Value(t) => t,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<DateTime<Utc>>> for CockroachBinarySourcePartitionParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<DateTime<Utc>> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            Some(postgres::types::Timestamp::PosInfinity) => Some(DateTime::<Utc>::MAX_UTC),
            Some(postgres::types::Timestamp::NegInfinity) => Some(DateTime::<Utc>::MIN_UTC),
            Some(postgres::types::Timestamp::Value(t)) => t,
            None => None,
        }
    }
}

impl<'r, 'a> Produce<'r, NaiveDate> for CockroachBinarySourcePartitionParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveDate {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            postgres::types::Date::PosInfinity => NaiveDate::MAX,
            postgres::types::Date::NegInfinity => NaiveDate::MIN,
            postgres::types::Date::Value(t) => t,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveDate>> for CockroachBinarySourcePartitionParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveDate> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            Some(postgres::types::Date::PosInfinity) => Some(NaiveDate::MAX),
            Some(postgres::types::Date::NegInfinity) => Some(NaiveDate::MIN),
            Some(postgres::types::Date::Value(t)) => t,
            None => None,
        }
    }
}

impl<'r, 'a> Produce<'r, HashMap<String, Option<String>>>
    for CockroachBinarySourcePartitionParser<'a>
{
    type Error = CockroachSourceError;
    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> HashMap<String, Option<String>> {
        unimplemented!("Please use `cursor` protocol for hstore type");
    }
}

impl<'r, 'a> Produce<'r, Option<HashMap<String, Option<String>>>>
    for CockroachBinarySourcePartitionParser<'a>
{
    type Error = CockroachSourceError;
    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<HashMap<String, Option<String>>> {
        unimplemented!("Please use `cursor` protocol for hstore type");
    }
}

pub struct CockroachCSVSourceParser<'a> {
    iter: StringRecordsIntoIter<CopyOutReader<'a>>,
    rowbuf: Vec<StringRecord>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> CockroachCSVSourceParser<'a> {
    pub fn new(
        iter: StringRecordsIntoIter<CopyOutReader<'a>>,
        schema: &[CockroachTypeSystem],
    ) -> Self {
        Self {
            iter,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(CockroachSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;
    type TypeSystem = CockroachTypeSystem;

    #[throws(CockroachSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        if !self.rowbuf.is_empty() {
            self.rowbuf.drain(..);
        }
        for _ in 0..DB_BUFFER_SIZE {
            if let Some(row) = self.iter.next() {
                self.rowbuf.push(row?);
            } else {
                self.is_finished = true;
                break;
            }
        }
        self.current_row = 0;
        self.current_col = 0;
        (self.rowbuf.len(), self.is_finished)
    }
}

macro_rules! impl_csv_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for CockroachCSVSourceParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    self.rowbuf[ridx][cidx].parse().map_err(|_| {
                        ConnectorXError::cannot_produce::<$t>(Some(self.rowbuf[ridx][cidx].into()))
                    })?
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for CockroachCSVSourceParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    match &self.rowbuf[ridx][cidx][..] {
                        "" => None,
                        v => Some(v.parse().map_err(|_| {
                            ConnectorXError::cannot_produce::<$t>(Some(self.rowbuf[ridx][cidx].into()))
                        })?),
                    }
                }
            }
        )+
    };
}

impl_csv_produce!(i8, i16, i32, i64, f32, f64, Uuid,);

macro_rules! impl_csv_vec_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, Vec<$t>> for CockroachCSVSourceParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&mut self) -> Vec<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let s = &self.rowbuf[ridx][cidx][..];
                    match s {
                        "{}" => vec![],
                        _ if s.len() < 3 => throw!(ConnectorXError::cannot_produce::<$t>(Some(s.into()))),
                        s => s[1..s.len() - 1]
                            .split(",")
                            .map(|v| {
                                v.parse()
                                    .map_err(|_| ConnectorXError::cannot_produce::<$t>(Some(s.into())))
                            })
                            .collect::<Result<Vec<$t>, ConnectorXError>>()?,
                    }
                }
            }

            impl<'r, 'a> Produce<'r, Option<Vec<$t>>> for CockroachCSVSourceParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&mut self) -> Option<Vec<$t>> {
                    let (ridx, cidx) = self.next_loc()?;
                    let s = &self.rowbuf[ridx][cidx][..];
                    match s {
                        "" => None,
                        "{}" => Some(vec![]),
                        _ if s.len() < 3 => throw!(ConnectorXError::cannot_produce::<$t>(Some(s.into()))),
                        s => Some(
                            s[1..s.len() - 1]
                                .split(",")
                                .map(|v| {
                                    v.parse()
                                        .map_err(|_| ConnectorXError::cannot_produce::<$t>(Some(s.into())))
                                })
                                .collect::<Result<Vec<$t>, ConnectorXError>>()?,
                        ),
                    }
                }
            }
        )+
    };
}

impl_csv_vec_produce!(i8, i16, i32, i64, f32, f64, Decimal, String,);

impl<'r, 'a> Produce<'r, HashMap<String, Option<String>>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;
    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> HashMap<String, Option<String>> {
        unimplemented!("Please use `cursor` protocol for hstore type");
    }
}

impl<'r, 'a> Produce<'r, Option<HashMap<String, Option<String>>>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;
    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<HashMap<String, Option<String>>> {
        unimplemented!("Please use `cursor` protocol for hstore type");
    }
}

impl<'r, 'a> Produce<'r, bool> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> bool {
        let (ridx, cidx) = self.next_loc()?;
        let ret = match &self.rowbuf[ridx][cidx][..] {
            "t" => true,
            "f" => false,
            _ => throw!(ConnectorXError::cannot_produce::<bool>(Some(
                self.rowbuf[ridx][cidx].into()
            ))),
        };
        ret
    }
}

impl<'r, 'a> Produce<'r, Option<bool>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<bool> {
        let (ridx, cidx) = self.next_loc()?;
        let ret = match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            "t" => Some(true),
            "f" => Some(false),
            _ => throw!(ConnectorXError::cannot_produce::<bool>(Some(
                self.rowbuf[ridx][cidx].into()
            ))),
        };
        ret
    }
}

impl<'r, 'a> Produce<'r, Vec<bool>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Vec<bool> {
        let (ridx, cidx) = self.next_loc()?;
        let s = &self.rowbuf[ridx][cidx][..];
        match s {
            "{}" => vec![],
            _ if s.len() < 3 => throw!(ConnectorXError::cannot_produce::<bool>(Some(s.into()))),
            s => s[1..s.len() - 1]
                .split(',')
                .map(|v| match v {
                    "t" => Ok(true),
                    "f" => Ok(false),
                    _ => throw!(ConnectorXError::cannot_produce::<bool>(Some(s.into()))),
                })
                .collect::<Result<Vec<bool>, ConnectorXError>>()?,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<Vec<bool>>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<Vec<bool>> {
        let (ridx, cidx) = self.next_loc()?;
        let s = &self.rowbuf[ridx][cidx][..];
        match s {
            "" => None,
            "{}" => Some(vec![]),
            _ if s.len() < 3 => throw!(ConnectorXError::cannot_produce::<bool>(Some(s.into()))),
            s => Some(
                s[1..s.len() - 1]
                    .split(',')
                    .map(|v| match v {
                        "t" => Ok(true),
                        "f" => Ok(false),
                        _ => throw!(ConnectorXError::cannot_produce::<bool>(Some(s.into()))),
                    })
                    .collect::<Result<Vec<bool>, ConnectorXError>>()?,
            ),
        }
    }
}

impl<'r, 'a> Produce<'r, Decimal> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Decimal {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "Infinity" => Decimal::MAX,
            "-Infinity" => Decimal::MIN,
            v => v
                .parse()
                .map_err(|_| ConnectorXError::cannot_produce::<Decimal>(Some(v.into())))?,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<Decimal>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<Decimal> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            "Infinity" => Some(Decimal::MAX),
            "-Infinity" => Some(Decimal::MIN),
            v => Some(
                v.parse()
                    .map_err(|_| ConnectorXError::cannot_produce::<Decimal>(Some(v.into())))?,
            ),
        }
    }
}

impl<'r, 'a> Produce<'r, DateTime<Utc>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> DateTime<Utc> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "infinity" => DateTime::<Utc>::MAX_UTC,
            "-infinity" => DateTime::<Utc>::MIN_UTC,
            // postgres csv return example: 1970-01-01 00:00:01+00
            v => format!("{}:00", v)
                .parse()
                .map_err(|_| ConnectorXError::cannot_produce::<DateTime<Utc>>(Some(v.into())))?,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<DateTime<Utc>>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<DateTime<Utc>> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            "infinity" => Some(DateTime::<Utc>::MAX_UTC),
            "-infinity" => Some(DateTime::<Utc>::MIN_UTC),
            v => {
                // postgres csv return example: 1970-01-01 00:00:01+00
                Some(format!("{}:00", v).parse().map_err(|_| {
                    ConnectorXError::cannot_produce::<DateTime<Utc>>(Some(v.into()))
                })?)
            }
        }
    }
}

impl<'r, 'a> Produce<'r, NaiveDate> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> NaiveDate {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "infinity" => NaiveDate::MAX,
            "-infinity" => NaiveDate::MIN,
            v => NaiveDate::parse_from_str(v, "%Y-%m-%d")
                .map_err(|_| ConnectorXError::cannot_produce::<NaiveDate>(Some(v.into())))?,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveDate>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<NaiveDate> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            "infinity" => Some(NaiveDate::MAX),
            "-infinity" => Some(NaiveDate::MIN),
            v => Some(
                NaiveDate::parse_from_str(v, "%Y-%m-%d")
                    .map_err(|_| ConnectorXError::cannot_produce::<NaiveDate>(Some(v.into())))?,
            ),
        }
    }
}

impl<'r, 'a> Produce<'r, NaiveDateTime> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> NaiveDateTime {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx] {
            "infinity" => NaiveDateTime::MAX,
            "-infinity" => NaiveDateTime::MIN,
            v => NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| ConnectorXError::cannot_produce::<NaiveDateTime>(Some(v.into())))?,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveDateTime>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<NaiveDateTime> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            "infinity" => Some(NaiveDateTime::MAX),
            "-infinity" => Some(NaiveDateTime::MIN),
            v => Some(
                NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S").map_err(|_| {
                    ConnectorXError::cannot_produce::<NaiveDateTime>(Some(v.into()))
                })?,
            ),
        }
    }
}

impl<'r, 'a> Produce<'r, NaiveTime> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> NaiveTime {
        let (ridx, cidx) = self.next_loc()?;
        NaiveTime::parse_from_str(&self.rowbuf[ridx][cidx], "%H:%M:%S").map_err(|_| {
            ConnectorXError::cannot_produce::<NaiveTime>(Some(self.rowbuf[ridx][cidx].into()))
        })?
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveTime>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&mut self) -> Option<NaiveTime> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            v => Some(
                NaiveTime::parse_from_str(v, "%H:%M:%S")
                    .map_err(|_| ConnectorXError::cannot_produce::<NaiveTime>(Some(v.into())))?,
            ),
        }
    }
}

impl<'r, 'a> Produce<'r, &'r str> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> &'r str {
        let (ridx, cidx) = self.next_loc()?;
        &self.rowbuf[ridx][cidx]
    }
}

impl<'r, 'a> Produce<'r, Option<&'r str>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<&'r str> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            v => Some(v),
        }
    }
}

impl<'r, 'a> Produce<'r, Vec<u8>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Vec<u8> {
        let (ridx, cidx) = self.next_loc()?;
        decode(&self.rowbuf[ridx][cidx][2..])? // escape \x in the beginning
    }
}

impl<'r, 'a> Produce<'r, Option<Vec<u8>>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<Vec<u8>> {
        let (ridx, cidx) = self.next_loc()?;
        match &self.rowbuf[ridx][cidx] {
            // escape \x in the beginning, empty if None
            "" => None,
            v => Some(decode(&v[2..])?),
        }
    }
}

impl<'r, 'a> Produce<'r, Value> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Value {
        let (ridx, cidx) = self.next_loc()?;
        let v = &self.rowbuf[ridx][cidx];
        from_str(v).map_err(|_| ConnectorXError::cannot_produce::<Value>(Some(v.into())))?
    }
}

impl<'r, 'a> Produce<'r, Option<Value>> for CockroachCSVSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<Value> {
        let (ridx, cidx) = self.next_loc()?;

        match &self.rowbuf[ridx][cidx][..] {
            "" => None,
            v => {
                from_str(v).map_err(|_| ConnectorXError::cannot_produce::<Value>(Some(v.into())))?
            }
        }
    }
}

pub struct CockroachRawSourceParser<'a> {
    iter: RowIter<'a>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> CockroachRawSourceParser<'a> {
    pub fn new(iter: RowIter<'a>, schema: &[CockroachTypeSystem]) -> Self {
        Self {
            iter,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(CockroachSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for CockroachRawSourceParser<'a> {
    type TypeSystem = CockroachTypeSystem;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        if !self.rowbuf.is_empty() {
            self.rowbuf.drain(..);
        }
        for _ in 0..DB_BUFFER_SIZE {
            if let Some(row) = self.iter.next()? {
                self.rowbuf.push(row);
            } else {
                self.is_finished = true;
                break;
            }
        }
        self.current_row = 0;
        self.current_col = 0;
        (self.rowbuf.len(), self.is_finished)
    }
}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for CockroachRawSourceParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let row = &self.rowbuf[ridx];
                    let val = row.try_get(cidx)?;
                    val
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for CockroachRawSourceParser<'a> {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let row = &self.rowbuf[ridx];
                    let val = row.try_get(cidx)?;
                    val
                }
            }
        )+
    };
}

impl_produce!(
    i8,
    i16,
    i32,
    i64,
    f32,
    f64,
    Decimal,
    Vec<i16>,
    Vec<i32>,
    Vec<i64>,
    Vec<f32>,
    Vec<f64>,
    Vec<Decimal>,
    bool,
    Vec<bool>,
    &'r str,
    Vec<u8>,
    NaiveTime,
    Uuid,
    Value,
    HashMap<String, Option<String>>,
    Vec<String>,
);

impl<'r, 'a> Produce<'r, DateTime<Utc>> for CockroachRawSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> DateTime<Utc> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val: postgres::types::Timestamp<DateTime<Utc>> = row.try_get(cidx)?;
        match val {
            postgres::types::Timestamp::PosInfinity => DateTime::<Utc>::MAX_UTC,
            postgres::types::Timestamp::NegInfinity => DateTime::<Utc>::MIN_UTC,
            postgres::types::Timestamp::Value(t) => t,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<DateTime<Utc>>> for CockroachRawSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<DateTime<Utc>> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            Some(postgres::types::Timestamp::PosInfinity) => Some(DateTime::<Utc>::MAX_UTC),
            Some(postgres::types::Timestamp::NegInfinity) => Some(DateTime::<Utc>::MIN_UTC),
            Some(postgres::types::Timestamp::Value(t)) => t,
            None => None,
        }
    }
}

impl<'r, 'a> Produce<'r, NaiveDateTime> for CockroachRawSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveDateTime {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val: postgres::types::Timestamp<NaiveDateTime> = row.try_get(cidx)?;
        match val {
            postgres::types::Timestamp::PosInfinity => NaiveDateTime::MAX,
            postgres::types::Timestamp::NegInfinity => NaiveDateTime::MIN,
            postgres::types::Timestamp::Value(t) => t,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveDateTime>> for CockroachRawSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveDateTime> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            Some(postgres::types::Timestamp::PosInfinity) => Some(NaiveDateTime::MAX),
            Some(postgres::types::Timestamp::NegInfinity) => Some(NaiveDateTime::MIN),
            Some(postgres::types::Timestamp::Value(t)) => t,
            None => None,
        }
    }
}

impl<'r, 'a> Produce<'r, NaiveDate> for CockroachRawSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveDate {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val: postgres::types::Date<NaiveDate> = row.try_get(cidx)?;
        match val {
            postgres::types::Date::PosInfinity => NaiveDate::MAX,
            postgres::types::Date::NegInfinity => NaiveDate::MIN,
            postgres::types::Date::Value(t) => t,
        }
    }
}

impl<'r, 'a> Produce<'r, Option<NaiveDate>> for CockroachRawSourceParser<'a> {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveDate> {
        let (ridx, cidx) = self.next_loc()?;
        let row = &self.rowbuf[ridx];
        let val = row.try_get(cidx)?;
        match val {
            Some(postgres::types::Date::PosInfinity) => Some(NaiveDate::MAX),
            Some(postgres::types::Date::NegInfinity) => Some(NaiveDate::MIN),
            Some(postgres::types::Date::Value(t)) => t,
            None => None,
        }
    }
}

impl<C> SourcePartition for CockroachSourcePartition<SimpleProtocol, C>
where
    C: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    C::TlsConnect: Send,
    C::Stream: Send,
    <C::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type TypeSystem = CockroachTypeSystem;
    type Parser<'a> = CockroachSimpleSourceParser;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn result_rows(&mut self) {
        self.nrows = get_total_rows(&mut self.conn, &self.query)?;
    }

    #[throws(CockroachSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let rows = self.conn.simple_query(self.query.as_str())?; // unless reading the data, it seems like issue the query is fast
        CockroachSimpleSourceParser::new(rows, &self.schema)
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

pub struct CockroachSimpleSourceParser {
    rows: Vec<SimpleQueryMessage>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
}
impl<'a> CockroachSimpleSourceParser {
    pub fn new(rows: Vec<SimpleQueryMessage>, schema: &[CockroachTypeSystem]) -> Self {
        Self {
            rows,
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
        }
    }

    #[throws(CockroachSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for CockroachSimpleSourceParser {
    type TypeSystem = CockroachTypeSystem;
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        self.current_row = 0;
        self.current_col = 0;
        (self.rows.len() - 1, true) // last message is command complete
    }
}

macro_rules! impl_simple_produce_unimplemented {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for CockroachSimpleSourceParser {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> $t {
                   unimplemented!("not implemented!");
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for CockroachSimpleSourceParser {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                   unimplemented!("not implemented!");
                }
            }
        )+
    };
}

macro_rules! impl_simple_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r> Produce<'r, $t> for CockroachSimpleSourceParser {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let val = match &self.rows[ridx] {
                        SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                            Some(s) => s
                                .parse()
                                .map_err(|_| ConnectorXError::cannot_produce::<$t>(Some(s.into())))?,
                            None => throw!(anyhow!(
                                "Cannot parse NULL in NOT NULL column."
                            )),
                        },
                        SimpleQueryMessage::CommandComplete(c) => {
                            panic!("get command: {}", c);
                        }
                        _ => {
                            panic!("what?");
                        }
                    };
                    val
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for CockroachSimpleSourceParser {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let val = match &self.rows[ridx] {
                        SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                            Some(s) => Some(
                                s.parse()
                                    .map_err(|_| ConnectorXError::cannot_produce::<$t>(Some(s.into())))?,
                            ),
                            None => None,
                        },
                        SimpleQueryMessage::CommandComplete(c) => {
                            panic!("get command: {}", c);
                        }
                        _ => {
                            panic!("what?");
                        }
                    };
                    val
                }
            }
        )+
    };
}

impl_simple_produce!(i8, i16, i32, i64, f32, f64, Uuid, bool,);

impl<'r> Produce<'r, Decimal> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Decimal {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some("Infinity") => Decimal::MAX,
                Some("-Infinity") => Decimal::MIN,
                Some(s) => s
                    .parse()
                    .map_err(|_| ConnectorXError::cannot_produce::<Decimal>(Some(s.into())))?,
                None => throw!(anyhow!("Cannot parse NULL in NOT NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r, 'a> Produce<'r, Option<Decimal>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<Decimal> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some("Infinity") => Some(Decimal::MAX),
                Some("-Infinity") => Some(Decimal::MIN),
                Some(s) => Some(
                    s.parse()
                        .map_err(|_| ConnectorXError::cannot_produce::<Decimal>(Some(s.into())))?,
                ),
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl_simple_produce_unimplemented!(
    Value,
    HashMap<String, Option<String>>,);

impl<'r> Produce<'r, &'r str> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> &'r str {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => s,
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r, 'a> Produce<'r, Option<&'r str>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<&'r str> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => row.try_get(cidx)?,
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, Vec<u8>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Vec<u8> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => {
                    let mut res = s.chars();
                    res.next();
                    res.next();
                    decode(
                        res.enumerate()
                            .fold(String::new(), |acc, (_i, c)| format!("{}{}", acc, c))
                            .chars()
                            .map(|c| c as u8)
                            .collect::<Vec<u8>>(),
                    )?
                }
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r, 'a> Produce<'r, Option<Vec<u8>>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<Vec<u8>> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => {
                    let mut res = s.chars();
                    res.next();
                    res.next();
                    Some(decode(
                        res.enumerate()
                            .fold(String::new(), |acc, (_i, c)| format!("{}{}", acc, c))
                            .chars()
                            .map(|c| c as u8)
                            .collect::<Vec<u8>>(),
                    )?)
                }
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

fn rem_first_and_last(value: &str) -> &str {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str()
}

macro_rules! impl_simple_vec_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r> Produce<'r, Vec<$t>> for CockroachSimpleSourceParser {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Vec<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let val = match &self.rows[ridx] {
                        SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                            Some(s) => match s{
                                "" => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
                                "{}" => vec![],
                                _ => rem_first_and_last(s).split(",").map(|token| token.parse().map_err(|_| ConnectorXError::cannot_produce::<Vec<$t>>(Some(s.into())))).collect::<Result<Vec<$t>, ConnectorXError>>()?
                            },
                            None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
                        },
                        SimpleQueryMessage::CommandComplete(c) => {
                            panic!("get command: {}", c);
                        }
                        _ => {
                            panic!("what?");
                        }
                    };
                    val
                }
            }

            impl<'r, 'a> Produce<'r, Option<Vec<$t>>> for CockroachSimpleSourceParser {
                type Error = CockroachSourceError;

                #[throws(CockroachSourceError)]
                fn produce(&'r mut self) -> Option<Vec<$t>> {
                    let (ridx, cidx) = self.next_loc()?;
                    let val = match &self.rows[ridx] {

                        SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                            Some(s) => match s{
                                "" => None,
                                "{}" => Some(vec![]),
                                _ => Some(rem_first_and_last(s).split(",").map(|token| token.parse().map_err(|_| ConnectorXError::cannot_produce::<Vec<$t>>(Some(s.into())))).collect::<Result<Vec<$t>, ConnectorXError>>()?)
                            },
                            None => None,
                        },

                        SimpleQueryMessage::CommandComplete(c) => {
                            panic!("get command: {}", c);
                        }
                        _ => {
                            panic!("what?");
                        }
                    };
                    val
                }
            }
        )+
    };
}
impl_simple_vec_produce!(i16, i32, i64, f32, f64, Decimal, String,);

impl<'r> Produce<'r, Vec<bool>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Vec<bool> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => match s {
                    "" => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
                    "{}" => vec![],
                    _ => rem_first_and_last(s)
                        .split(',')
                        .map(|token| match token {
                            "t" => Ok(true),
                            "f" => Ok(false),
                            _ => {
                                throw!(ConnectorXError::cannot_produce::<Vec<bool>>(Some(s.into())))
                            }
                        })
                        .collect::<Result<Vec<bool>, ConnectorXError>>()?,
                },
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, Option<Vec<bool>>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<Vec<bool>> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => match s {
                    "" => None,
                    "{}" => Some(vec![]),
                    _ => Some(
                        rem_first_and_last(s)
                            .split(',')
                            .map(|token| match token {
                                "t" => Ok(true),
                                "f" => Ok(false),
                                _ => throw!(ConnectorXError::cannot_produce::<Vec<bool>>(Some(
                                    s.into()
                                ))),
                            })
                            .collect::<Result<Vec<bool>, ConnectorXError>>()?,
                    ),
                },
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, NaiveDate> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveDate {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => match s {
                    "infinity" => NaiveDate::MAX,
                    "-infinity" => NaiveDate::MIN,
                    s => NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                        ConnectorXError::cannot_produce::<NaiveDate>(Some(s.into()))
                    })?,
                },
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, Option<NaiveDate>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveDate> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => match s {
                    "infinity" => Some(NaiveDate::MAX),
                    "-infinity" => Some(NaiveDate::MIN),
                    s => Some(NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                        ConnectorXError::cannot_produce::<Option<NaiveDate>>(Some(s.into()))
                    })?),
                },
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, NaiveTime> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveTime {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => NaiveTime::parse_from_str(s, "%H:%M:%S")
                    .map_err(|_| ConnectorXError::cannot_produce::<NaiveTime>(Some(s.into())))?,
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, Option<NaiveTime>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveTime> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => Some(NaiveTime::parse_from_str(s, "%H:%M:%S").map_err(|_| {
                    ConnectorXError::cannot_produce::<Option<NaiveTime>>(Some(s.into()))
                })?),
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, NaiveDateTime> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> NaiveDateTime {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => match s {
                    "infinity" => NaiveDateTime::MAX,
                    "-infinity" => NaiveDateTime::MIN,
                    s => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(|_| {
                        ConnectorXError::cannot_produce::<NaiveDateTime>(Some(s.into()))
                    })?,
                },
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, Option<NaiveDateTime>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<NaiveDateTime> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some(s) => match s {
                    "infinity" => Some(NaiveDateTime::MAX),
                    "-infinity" => Some(NaiveDateTime::MIN),
                    s => Some(
                        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(|_| {
                            ConnectorXError::cannot_produce::<Option<NaiveDateTime>>(Some(s.into()))
                        })?,
                    ),
                },
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, DateTime<Utc>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> DateTime<Utc> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some("infinity") => DateTime::<Utc>::MAX_UTC,
                Some("-infinity") => DateTime::<Utc>::MIN_UTC,
                Some(s) => {
                    let time_string = format!("{}:00", s).to_owned();
                    let slice: &str = &time_string[..];
                    let time: DateTime<FixedOffset> =
                        DateTime::parse_from_str(slice, "%Y-%m-%d %H:%M:%S%:z").unwrap();

                    time.with_timezone(&Utc)
                }
                None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}

impl<'r> Produce<'r, Option<DateTime<Utc>>> for CockroachSimpleSourceParser {
    type Error = CockroachSourceError;

    #[throws(CockroachSourceError)]
    fn produce(&'r mut self) -> Option<DateTime<Utc>> {
        let (ridx, cidx) = self.next_loc()?;
        let val = match &self.rows[ridx] {
            SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
                Some("infinity") => Some(DateTime::<Utc>::MAX_UTC),
                Some("-infinity") => Some(DateTime::<Utc>::MIN_UTC),
                Some(s) => {
                    let time_string = format!("{}:00", s).to_owned();
                    let slice: &str = &time_string[..];
                    let time: DateTime<FixedOffset> =
                        DateTime::parse_from_str(slice, "%Y-%m-%d %H:%M:%S%:z").unwrap();

                    Some(time.with_timezone(&Utc))
                }
                None => None,
            },
            SimpleQueryMessage::CommandComplete(c) => {
                panic!("get command: {}", c);
            }
            _ => {
                panic!("what?");
            }
        };
        val
    }
}
