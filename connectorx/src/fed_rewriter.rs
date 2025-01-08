use crate::{
    constants::{CX_REWRITER_PATH, J4RS_BASE_PATH},
    prelude::*,
};
use fehler::throws;
use j4rs::{ClasspathEntry, Instance, InvocationArg, Jvm, JvmBuilder};
use log::debug;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::{env, fs};

pub struct Plan {
    pub db_name: String,
    pub db_alias: String,
    pub sql: String,
    pub cardinality: usize,
}

pub struct FederatedDataSourceInfo<'a> {
    pub conn_str_info: Option<SourceConn>,
    pub manual_info: Option<HashMap<String, Vec<String>>>,
    pub is_local: bool,
    pub jdbc_url: &'a str,
    pub jdbc_driver: &'a str,
}

impl<'a> FederatedDataSourceInfo<'a> {
    pub fn new_from_conn_str(
        source_conn: SourceConn,
        is_local: bool,
        jdbc_url: &'a str,
        jdbc_driver: &'a str,
    ) -> Self {
        Self {
            conn_str_info: Some(source_conn),
            manual_info: None,
            is_local,
            jdbc_url,
            jdbc_driver,
        }
    }
    pub fn new_from_manual_schema(
        manual_schema: HashMap<String, Vec<String>>,
        is_local: bool,
    ) -> Self {
        Self {
            conn_str_info: None,
            manual_info: Some(manual_schema),
            is_local,
            jdbc_url: "",
            jdbc_driver: "",
        }
    }
}

#[throws(ConnectorXOutError)]
fn init_jvm(j4rs_base: Option<&str>) -> Jvm {
    let base = match j4rs_base {
        Some(path) => fs::canonicalize(path)
            .map_err(|_| ConnectorXOutError::FileNotFoundError(path.to_string()))?,
        None => fs::canonicalize(J4RS_BASE_PATH)
            .map_err(|_| ConnectorXOutError::FileNotFoundError(J4RS_BASE_PATH.to_string()))?,
    };
    debug!("j4rs base path: {:?}", base);

    let rewriter_path = env::var("CX_REWRITER_PATH").unwrap_or(CX_REWRITER_PATH.to_string());
    let path = fs::canonicalize(rewriter_path.as_str())
        .map_err(|_| ConnectorXOutError::FileNotFoundError(rewriter_path))?;

    debug!("rewriter path: {:?}", path);

    let entry = ClasspathEntry::new(path.to_str().unwrap());
    JvmBuilder::new()
        .skip_setting_native_lib()
        .classpath_entry(entry)
        .with_base_path(base.to_str().unwrap())
        .build()?
}

#[allow(dead_code)]
#[throws(ConnectorXOutError)]
fn create_sources(
    jvm: &Jvm,
    db_map: &HashMap<String, FederatedDataSourceInfo>,
) -> (Instance, Instance) {
    debug!("Could not find environment variable `FED_CONFIG_PATH`, use manual configuration (c++ API only)!");
    let mut db_config = vec![];
    let db_manual = jvm.create_instance("java.util.HashMap", &[])?;

    for (db_name, db_info) in db_map.iter() {
        if db_info.manual_info.is_some() {
            let manual_info = db_info.manual_info.as_ref().unwrap();
            let schema_info = jvm.create_instance("java.util.HashMap", &[])?;
            for (name, columns) in manual_info {
                let arr_instance = jvm.java_list("java.lang.String", columns.to_vec())?;
                jvm.invoke(
                    &schema_info,
                    "put",
                    &[
                        InvocationArg::try_from(name).unwrap(),
                        InvocationArg::try_from(arr_instance).unwrap(),
                    ],
                )?;
            }
            let fed_ds = jvm.create_instance(
                "ai.dataprep.federated.FederatedDataSource",
                &[
                    InvocationArg::try_from(db_info.is_local).unwrap(),
                    InvocationArg::try_from(schema_info).unwrap(),
                ],
            )?;
            jvm.invoke(
                &db_manual,
                "put",
                &[
                    InvocationArg::try_from(db_name).unwrap(),
                    InvocationArg::try_from(fed_ds).unwrap(),
                ],
            )?;
        } else {
            db_config.push(String::from(db_name));
        }
    }
    let db_config = jvm.java_list("java.lang.String", db_config)?;
    (db_config, db_manual)
}

#[allow(dead_code)]
#[throws(ConnectorXOutError)]
fn create_sources2(
    jvm: &Jvm,
    db_map: &HashMap<String, FederatedDataSourceInfo>,
) -> (Instance, Instance) {
    debug!("Found environment variable `FED_CONFIG_PATH`, use configurations!");
    let mut dbs = vec![];
    let db_manual = jvm.create_instance("java.util.HashMap", &[])?;
    for db in db_map.keys() {
        dbs.push(String::from(db));
    }
    (jvm.java_list("java.lang.String", dbs)?, db_manual)
}

//主要功能是将一个 SQL 查询重写为多个子查询，以便在不同的数据源上执行。
// 这个函数使用了 Java 4 Rust (j4rs) 库来与 Java 虚拟机 (JVM) 交互，从而调用 Java 代码来完成 SQL 重写
#[throws(ConnectorXOutError)]
pub fn rewrite_sql(
    sql: &str,
    db_map: &HashMap<String, FederatedDataSourceInfo>,
    j4rs_base: Option<&str>, //可选的 Java 4 Rust 基础路径，用于初始化 JVM.
    strategy: &str, //用于 SQL 重写的策略字符串.
) -> Vec<Plan> {

    //使用 init_jvm 函数初始化 JVM，传入 j4rs_base 作为参数.
    // 打印调试信息，表示 JVM 初始化成功.
    let jvm = init_jvm(j4rs_base)?;
    debug!("init jvm successfully!");

    let sql = InvocationArg::try_from(sql).unwrap();
    let strategy = InvocationArg::try_from(strategy).unwrap();

    //根据环境变量 FED_CONFIG_PATH 是否存在，调用 create_sources2 或 create_sources 函数来创建数据库配置 (db_config) 和手动配置 (db_manual)。
    // 这两个函数的具体实现未在代码中给出，但它们应该负责将 Rust 中的数据库连接信息转换为 Java 可以理解的格式.
    let (db_config, db_manual) = match env::var("FED_CONFIG_PATH") {
        Ok(_) => create_sources2(&jvm, db_map)?,
        _ => create_sources(&jvm, db_map)?,
    };

    //创建重写器实例：
    //
    // 使用 JVM 创建一个 FederatedQueryRewriter 实例，这个实例是 Java 类 ai.dataprep.accio.FederatedQueryRewriter 的对象.
    // 将 SQL 查询、数据库配置、手动配置和策略作为参数传递给重写器的 rewrite 方法，得到重写后的查询计划 (plan)。
    let rewriter = jvm.create_instance("ai.dataprep.accio.FederatedQueryRewriter", &[])?;
    let db_config = InvocationArg::try_from(db_config).unwrap();
    let db_manual = InvocationArg::try_from(db_manual).unwrap();
    let plan = jvm.invoke(&rewriter, "rewrite", &[sql, db_config, db_manual, strategy])?;

    //获取查询计划信息：
    //
    // 从查询计划中获取子查询的数量 (count)。
    // 打印调试信息，表示重写完成并获取到的子查询数量.
    // 遍历每个子查询，获取其数据库名称 (db)、别名 (alias_db)、SQL 语句 (rewrite_sql) 和基数 (cardinality)。
    // 将这些信息封装到 Plan 结构体中，并将其添加到 fed_plan 向量中.
    let count = jvm.invoke(&plan, "getCount", &[])?;
    let count: i32 = jvm.to_rust(count)?;
    debug!("rewrite finished, got {} queries", count);

    let mut fed_plan = vec![];
    for i in 0..count {
        let idx = [InvocationArg::try_from(i).unwrap().into_primitive()?];

        let db = jvm.invoke(&plan, "getDBName", &idx)?;
        let db: String = jvm.to_rust(db)?;

        let alias_db = jvm.invoke(&plan, "getAliasDBName", &idx)?;
        let alias_db: String = jvm.to_rust(alias_db)?;

        let rewrite_sql = jvm.invoke(&plan, "getSql", &idx)?;
        let rewrite_sql: String = jvm.to_rust(rewrite_sql)?;

        let cardinality = jvm.invoke(&plan, "getCardinality", &idx)?;
        let cardinality: usize = jvm.to_rust(cardinality)?;

        debug!(
            "{} - db: {}, alias: {}, cardinality: {}, rewrite sql: {}",
            i, db, alias_db, cardinality, rewrite_sql
        );
        fed_plan.push(Plan {
            db_name: db,
            db_alias: alias_db,
            sql: rewrite_sql,
            cardinality,
        });
    }
    fed_plan
}
//这个函数通过与 JVM 交互，
// 利用 Java 代码来重写 SQL 查询，生成多个子查询计划。每个子查询计划包含数据库名称、别名、SQL 语句和基数等信息，
// 这些信息将用于后续的联邦查询执行。通过这种方式，可以实现跨多个数据源的复杂查询操作.