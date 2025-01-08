use crate::errors::ConnectorXError;
#[cfg(feature = "src_oracle")]
use crate::sources::oracle::OracleDialect;
use fehler::{throw, throws};
use log::{debug, trace, warn};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Query, Select,
    SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value,
    WildcardAdditionalOptions,
};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
#[cfg(feature = "src_oracle")]
use std::any::Any;

#[derive(Debug, Clone)]
pub enum CXQuery<Q = String> {
    Naked(Q),   // The query directly comes from the user
    Wrapped(Q), // The user query is already wrapped in a subquery
}

impl<Q: std::fmt::Display> std::fmt::Display for CXQuery<Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CXQuery::Naked(q) => write!(f, "{}", q),
            CXQuery::Wrapped(q) => write!(f, "{}", q),
        }
    }
}

impl<Q: AsRef<str>> CXQuery<Q> {
    pub fn as_str(&self) -> &str {
        match self {
            CXQuery::Naked(q) => q.as_ref(),
            CXQuery::Wrapped(q) => q.as_ref(),
        }
    }
}

impl From<&str> for CXQuery {
    fn from(s: &str) -> CXQuery<String> {
        CXQuery::Naked(s.to_string())
    }
}

impl From<&&str> for CXQuery {
    fn from(s: &&str) -> CXQuery<String> {
        CXQuery::Naked(s.to_string())
    }
}

impl From<&String> for CXQuery {
    fn from(s: &String) -> CXQuery {
        CXQuery::Naked(s.clone())
    }
}

impl From<&CXQuery> for CXQuery {
    fn from(q: &CXQuery) -> CXQuery {
        q.clone()
    }
}

impl CXQuery<String> {
    pub fn naked<Q: AsRef<str>>(q: Q) -> Self {
        CXQuery::Naked(q.as_ref().to_string())
    }
}

impl<Q: AsRef<str>> AsRef<str> for CXQuery<Q> {
    fn as_ref(&self) -> &str {
        match self {
            CXQuery::Naked(q) => q.as_ref(),
            CXQuery::Wrapped(q) => q.as_ref(),
        }
    }
}

impl<Q> CXQuery<Q> {
    pub fn map<F, U>(&self, f: F) -> CXQuery<U>
    where
        F: Fn(&Q) -> U,
    {
        match self {
            CXQuery::Naked(q) => CXQuery::Naked(f(q)),
            CXQuery::Wrapped(q) => CXQuery::Wrapped(f(q)),
        }
    }
}

impl<Q, E> CXQuery<Result<Q, E>> {
    pub fn result(self) -> Result<CXQuery<Q>, E> {
        match self {
            CXQuery::Naked(q) => q.map(CXQuery::Naked),
            CXQuery::Wrapped(q) => q.map(CXQuery::Wrapped),
        }
    }
}

//将一个查询包装成一个派生表（derived table），并返回一个新的 SQL 语句
//query: 原始查询的可变引用，表示要被包装的查询.
// projection: 一个包含选择项（SelectItem）的向量，定义了新查询的输出列.
// selection: 一个可选的表达式（Option<Expr>），用于定义新查询的 WHERE 子句.
// tmp_tab_name: 一个字符串，表示派生表的别名.
// wrap a query into a derived table
fn wrap_query(
    query: &mut Query,
    projection: Vec<SelectItem>,
    selection: Option<Expr>,
    tmp_tab_name: &str,
) -> Statement {
    //将原始查询的 with 子句保存到一个变量 with 中.
    // 将原始查询的 with 字段设置为 None，以便在后续的派生表中使用.
    let with = query.with.clone();
    query.with = None;
    //如果 tmp_tab_name 是空字符串，则别名设置为 None.
    // 否则，创建一个 TableAlias 结构体，包含派生表的别名名称和空的列列表.
    let alias = if tmp_tab_name.is_empty() {
        None
    } else {
        Some(TableAlias {
            name: Ident {
                value: tmp_tab_name.into(),
                quote_style: None,
            },
            columns: vec![],
        })
    };
    //创建一个新的 Query 结构体
    Statement::Query(Box::new(Query {
        with,
        locks: vec![],
        //body: 使用 SetExpr::Select 包装一个新的 Select 语句
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: None,//distinct: 没有 DISTINCT 关键字，设置为 None.
            top: None,
            projection,//projection: 使用传入的 projection 向量.
            from: vec![TableWithJoins {//from: 包含一个 TableWithJoins 结构体，表示派生表
                relation: TableFactor::Derived {//relation: 使用 TableFactor::Derived 创建派生表，包含原始查询和别名.
                    lateral: false,
                    subquery: Box::new(query.clone()),
                    alias,
                },
                joins: vec![],//joins: 空的连接列表
            }],
            lateral_views: vec![],
            selection,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            into: None,
            named_window: vec![],
            qualify: None,
        }))),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }))
    //函数返回一个 Statement 类型的值，表示包装后的查询语句. 这个语句可以被进一步处理或直接用于数据库执行.
}

trait StatementExt {
    fn as_query(&self) -> Option<&Query>;
}

impl StatementExt for Statement {
    fn as_query(&self) -> Option<&Query> {
        match self {
            Statement::Query(q) => Some(q),
            _ => None,
        }
    }
}

trait QueryExt {
    fn as_select_mut(&mut self) -> Option<&mut Select>;
}

impl QueryExt for Query {
    fn as_select_mut(&mut self) -> Option<&mut Select> {
        match *self.body {
            SetExpr::Select(ref mut select) => Some(select),
            _ => None,
        }
    }
}

#[throws(ConnectorXError)]
pub fn count_query<T: Dialect>(sql: &CXQuery<String>, dialect: &T) -> CXQuery<String> {
    trace!("Incoming query: {}", sql);

    const COUNT_TMP_TAB_NAME: &str = "CXTMPTAB_COUNT";

    #[allow(unused_mut)]
    let mut table_alias = COUNT_TMP_TAB_NAME;

    // HACK: Some dialect (e.g. Oracle) does not support "AS" for alias
    #[cfg(feature = "src_oracle")]
    if dialect.type_id() == (OracleDialect {}.type_id()) {
        // table_alias = "";
        return CXQuery::Wrapped(format!(
            "SELECT COUNT(*) FROM ({}) {}",
            sql.as_str(),
            COUNT_TMP_TAB_NAME
        ));
    }

    let tsql = match sql.map(|sql| Parser::parse_sql(dialect, sql)).result() {
        Ok(ast) => {
            let projection = vec![SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "count".to_string(),
                    quote_style: None,
                }]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                over: None,
                distinct: false,
                order_by: vec![],
                special: false,
            }))];
            let ast_count: Statement = match ast {
                CXQuery::Naked(ast) => {
                    if ast.len() != 1 {
                        throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
                    }
                    let mut query = ast[0]
                        .as_query()
                        .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
                        .clone();
                    if query.offset.is_none() {
                        query.order_by = vec![]; // mssql offset must appear with order by
                    }
                    let select = query
                        .as_select_mut()
                        .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?;
                    select.sort_by = vec![];
                    wrap_query(&mut query, projection, None, table_alias)
                }
                CXQuery::Wrapped(ast) => {
                    if ast.len() != 1 {
                        throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
                    }
                    let mut query = ast[0]
                        .as_query()
                        .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
                        .clone();
                    let select = query
                        .as_select_mut()
                        .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?;
                    select.projection = projection;
                    Statement::Query(Box::new(query))
                }
            };
            format!("{}", ast_count)
        }
        Err(e) => {
            warn!("parser error: {:?}, manually compose query string", e);
            format!(
                "SELECT COUNT(*) FROM ({}) as {}",
                sql.as_str(),
                COUNT_TMP_TAB_NAME
            )
        }
    };

    debug!("Transformed count query: {}", tsql);
    CXQuery::Wrapped(tsql)
}

#[throws(ConnectorXError)]
pub fn limit1_query<T: Dialect>(sql: &CXQuery<String>, dialect: &T) -> CXQuery<String> {
    trace!("Incoming query: {}", sql);

    let sql = match Parser::parse_sql(dialect, sql.as_str()) {
        Ok(mut ast) => {
            if ast.len() != 1 {
                throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
            }

            match &mut ast[0] {
                Statement::Query(q) => {
                    q.limit = Some(Expr::Value(Value::Number("1".to_string(), false)));
                }
                _ => throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string())),
            };

            format!("{}", ast[0])
        }
        Err(e) => {
            warn!("parser error: {:?}, manually compose query string", e);
            format!("{} LIMIT 1", sql.as_str())
        }
    };

    debug!("Transformed limit 1 query: {}", sql);
    CXQuery::Wrapped(sql)
}

#[throws(ConnectorXError)]
#[cfg(feature = "src_oracle")]
pub fn limit1_query_oracle(sql: &CXQuery<String>) -> CXQuery<String> {
    trace!("Incoming oracle query: {}", sql);

    CXQuery::Wrapped(format!("SELECT * FROM ({}) WHERE rownum = 1", sql))

    // let ast = Parser::parse_sql(&OracleDialect {}, sql.as_str())?;
    // if ast.len() != 1 {
    //     throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
    // }
    // let ast_part: Statement;
    // let mut query = ast[0]
    //     .as_query()
    //     .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
    //     .clone();

    // let selection = Expr::BinaryOp {
    //     left: Box::new(Expr::CompoundIdentifier(vec![Ident {
    //         value: "rownum".to_string(),
    //         quote_style: None,
    //     }])),
    //     op: BinaryOperator::Eq,
    //     right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
    // };
    // ast_part = wrap_query(&mut query, vec![SelectItem::Wildcard], Some(selection), "");

    // let tsql = format!("{}", ast_part);
    // debug!("Transformed limit 1 query: {}", tsql);
    // CXQuery::Wrapped(tsql)
}

#[throws(ConnectorXError)]
pub fn single_col_partition_query<T: Dialect>(
    sql: &str,
    col: &str,
    lower: i64,
    upper: i64,
    dialect: &T,
) -> String {
    trace!("Incoming query: {}", sql);
    const PART_TMP_TAB_NAME: &str = "CXTMPTAB_PART";

    #[allow(unused_mut)]
    let mut table_alias = PART_TMP_TAB_NAME;
    #[allow(unused_mut)]
    let mut cid = Box::new(Expr::CompoundIdentifier(vec![
        Ident {
            value: PART_TMP_TAB_NAME.to_string(),
            quote_style: None,
        },
        Ident {
            value: col.to_string(),
            quote_style: None,
        },
    ]));

    // HACK: Some dialect (e.g. Oracle) does not support "AS" for alias
    #[cfg(feature = "src_oracle")]
    if dialect.type_id() == (OracleDialect {}.type_id()) {
        return format!("SELECT * FROM ({}) CXTMPTAB_PART WHERE CXTMPTAB_PART.{} >= {} AND CXTMPTAB_PART.{} < {}", sql, col, lower, col, upper);
        // table_alias = "";
        // cid = Box::new(Expr::Identifier(Ident {
        //     value: col.to_string(),
        //     quote_style: None,
        // }));
    }

    let tsql = match Parser::parse_sql(dialect, sql) {
        Ok(ast) => {
            if ast.len() != 1 {
                throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
            }

            let mut query = ast[0]
                .as_query()
                .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
                .clone();

            let select = query
                .as_select_mut()
                .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
                .clone();

            let ast_part: Statement;

            let lb = Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Number(lower.to_string(), false))),
                op: BinaryOperator::LtEq,
                right: cid.clone(),
            };

            let ub = Expr::BinaryOp {
                left: cid,
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Value(Value::Number(upper.to_string(), false))),
            };

            let selection = Expr::BinaryOp {
                left: Box::new(lb),
                op: BinaryOperator::And,
                right: Box::new(ub),
            };

            if query.limit.is_none() && select.top.is_none() && !query.order_by.is_empty() {
                // order by in a partition query does not make sense because partition is unordered.
                // clear the order by beceause mssql does not support order by in a derived table.
                // also order by in the derived table does not make any difference.
                query.order_by.clear();
            }

            ast_part = wrap_query(
                &mut query,
                vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
                Some(selection),
                table_alias,
            );
            format!("{}", ast_part)
        }
        Err(e) => {
            warn!("parser error: {:?}, manually compose query string", e);
            format!("SELECT * FROM ({}) AS CXTMPTAB_PART WHERE CXTMPTAB_PART.{} >= {} AND CXTMPTAB_PART.{} < {}", sql, col, lower, col, upper)
        }
    };

    debug!("Transformed single column partition query: {}", tsql);
    tsql
}
// 生成一个用于获取某个列的最小值和最大值的 SQL 查询字符串。这个函数使用了 Rust 的泛型和错误处理机制，并且考虑了不同数据库方言的兼容性。
//泛型参数：T: Dialect 表示 T 必须实现 Dialect trait，这意味着函数可以处理多种数据库方言.
// 返回类型：使用 #[throws(ConnectorXError)] 属性宏来指示函数可能会抛出 ConnectorXError 类型的错误.
// 参数：
// sql: 原始 SQL 查询字符串.
// col: 需要获取范围的列名.
// dialect: 数据库方言的实例，用于生成符合特定数据库语法的查询.
#[throws(ConnectorXError)]
pub fn get_partition_range_query<T: Dialect>(sql: &str, col: &str, dialect: &T) -> String {
    trace!("Incoming query: {}", sql);
    //RANGE_TMP_TAB_NAME: 用于在生成的查询中作为临时表的别名.
    // table_alias 和 args：分别用于存储表的别名和函数参数. 这些变量在某些情况下会被修改以适应特定的数据库方言.
    const RANGE_TMP_TAB_NAME: &str = "CXTMPTAB_RANGE";

    #[allow(unused_mut)]
    let mut table_alias = RANGE_TMP_TAB_NAME;
    //包含一个 FunctionArg 类型的元素，该元素表示一个未命名的函数参数，其具体表达式是一个复合标识符（Expr::CompoundIdentifier）。

    #[allow(unused_mut)]
    let mut args = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(//FunctionArg::Unnamed 表示这是一个未命名的函数参数. 在 SQL 中，未命名参数通常用于函数调用时不需要显式指定参数名称的情况.FunctionArgExpr::Expr 表示函数参数的具体表达式是一个 SQL 表达式（Expr）
        Expr::CompoundIdentifier(vec![//Expr::CompoundIdentifier 是一个复合标识符，用于表示一个由多个部分组成的标识符，例如在 SQL 中的表名和列名组合.在这里，它包含两个 Ident 结构体：
            Ident {//第一个 Ident 表示临时表的别名 RANGE_TMP_TAB_NAME，即 "CXTMPTAB_RANGE".
                value: RANGE_TMP_TAB_NAME.to_string(),
                quote_style: None,
            },
            Ident {//第二个 Ident 表示列名 col，这是函数的输入参数之一.
                value: col.to_string(),
                quote_style: None,
            },
        ]),
    ))];

    //对于 Oracle 数据库，由于其不支持使用 AS 关键字为别名命名，因此有一个特殊的处理分支：
    // 直接返回一个格式化的 SQL 字符串，该字符串使用 Oracle 兼容的语法来获取列的最小值和最大值.
    // 注释掉的代码展示了如何修改 table_alias 和 args，但实际并未使用.
    // HACK: Some dialect (e.g. Oracle) does not support "AS" for alias
    #[cfg(feature = "src_oracle")]
    if dialect.type_id() == (OracleDialect {}.type_id()) {
        return format!(
            "SELECT MIN({}.{}) as min, MAX({}.{}) as max FROM ({}) {}",
            RANGE_TMP_TAB_NAME, col, RANGE_TMP_TAB_NAME, col, sql, RANGE_TMP_TAB_NAME
        );
        // table_alias = "";
        // args = vec![FunctionArg::Unnamed(Expr::Identifier(Ident {
        //     value: col.to_string(),
        //     quote_style: None,
        // }))];
    }

    //使用 Parser::parse_sql 尝试解析输入的 SQL 字符串：
    let tsql = match Parser::parse_sql(dialect, sql) {
        Ok(ast) => {
            //如果解析成功且只包含一个查询语句，则对该查询进行处理
            if ast.len() != 1 {
                throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
            }

            //如果原始查询没有 LIMIT 和 OFFSET，则移除 ORDER BY 子句.
            // 创建一个新的查询，包含两个选择项：一个用于计算列的最小值，另一个用于计算最大值.
            // 使用 wrap_query 函数将原始查询包装成子查询，并应用新的选择项和别名.
            // 将生成的查询转换为字符串并返回.
            let mut query = ast[0]
                .as_query()
                .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
                .clone();
            let ast_range: Statement;

            if query.limit.is_none() && query.offset.is_none() {
                query.order_by = vec![]; // only omit orderby when there is no limit and offset in the query
            }
            let projection = vec![
                //最小值选择项：
                // 使用 SelectItem::UnnamedExpr 创建一个未命名的选择项.
                // Expr::Function 创建一个函数表达式，用于调用 MIN 函数：
                // name: 函数名 "min"，使用 ObjectName 包装.
                // args: 函数参数，使用前面定义的 args 向量.
                // over: 没有窗口定义，设置为 None.
                // distinct: 不使用 DISTINCT，设置为 false.
                // order_by: 没有排序，设置为空向量.
                // special: 不是特殊函数，设置为 false.
                SelectItem::UnnamedExpr(Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: "min".to_string(),
                        quote_style: None,
                    }]),
                    args: args.clone(),
                    over: None,
                    distinct: false,
                    order_by: vec![],
                    special: false,
                })),
                SelectItem::UnnamedExpr(Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: "max".to_string(),
                        quote_style: None,
                    }]),
                    args,
                    over: None,
                    distinct: false,
                    order_by: vec![],
                    special: false,
                })),
            ];
            ast_range = wrap_query(&mut query, projection, None, table_alias);
            format!("{}", ast_range)
        }
        Err(e) => {
            warn!("parser error: {:?}, manually compose query string", e);
            format!(
                "SELECT MIN({}.{}) as min, MAX({}.{}) as max FROM ({}) AS {}",
                RANGE_TMP_TAB_NAME, col, RANGE_TMP_TAB_NAME, col, sql, RANGE_TMP_TAB_NAME
            )
        }
    };

    //使用 trace! 和 debug! 宏记录函数的输入和输出信息，以便于调试和追踪.
    debug!("Transformed partition range query: {}", tsql);
    tsql
}

#[throws(ConnectorXError)]
pub fn get_partition_range_query_sep<T: Dialect>(
    sql: &str,
    col: &str,
    dialect: &T,
) -> (String, String) {
    trace!("Incoming query: {}", sql);
    const RANGE_TMP_TAB_NAME: &str = "CXTMPTAB_RANGE";

    let (sql_min, sql_max) = match Parser::parse_sql(dialect, sql) {
        Ok(ast) => {
            if ast.len() != 1 {
                throw!(ConnectorXError::SqlQueryNotSupported(sql.to_string()));
            }

            let mut query = ast[0]
                .as_query()
                .ok_or_else(|| ConnectorXError::SqlQueryNotSupported(sql.to_string()))?
                .clone();

            let ast_range_min: Statement;
            let ast_range_max: Statement;

            query.order_by = vec![];
            let min_proj = vec![SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "min".to_string(),
                    quote_style: None,
                }]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::CompoundIdentifier(vec![
                        Ident {
                            value: RANGE_TMP_TAB_NAME.to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: col.to_string(),
                            quote_style: None,
                        },
                    ]),
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                special: false,
            }))];
            let max_proj = vec![SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "max".to_string(),
                    quote_style: None,
                }]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::CompoundIdentifier(vec![
                        Ident {
                            value: RANGE_TMP_TAB_NAME.into(),
                            quote_style: None,
                        },
                        Ident {
                            value: col.into(),
                            quote_style: None,
                        },
                    ]),
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                special: false,
            }))];
            ast_range_min = wrap_query(&mut query.clone(), min_proj, None, RANGE_TMP_TAB_NAME);
            ast_range_max = wrap_query(&mut query, max_proj, None, RANGE_TMP_TAB_NAME);
            (format!("{}", ast_range_min), format!("{}", ast_range_max))
        }
        Err(e) => {
            warn!("parser error: {:?}, manually compose query string", e);
            (
                format!(
                    "SELECT MIN({}.{}) as min FROM ({}) AS {}",
                    RANGE_TMP_TAB_NAME, col, sql, RANGE_TMP_TAB_NAME
                ),
                format!(
                    "SELECT MAX({}.{}) as max FROM ({}) AS {}",
                    RANGE_TMP_TAB_NAME, col, sql, RANGE_TMP_TAB_NAME
                ),
            )
        }
    };
    debug!(
        "Transformed separated partition range query: {}, {}",
        sql_min, sql_max
    );
    (sql_min, sql_max)
}
