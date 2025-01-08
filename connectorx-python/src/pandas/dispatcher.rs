use super::{destination::PandasDestination, typesystem::PandasTypeSystem};
use crate::errors::ConnectorXPythonError;
use connectorx::errors::Result as CXResult;
use connectorx::prelude::*;
use itertools::Itertools;
use log::debug;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::marker::PhantomData;

pub struct PandasDispatcher<'py, S, TP> {
    src: S,
    dst: PandasDestination<'py>,
    queries: Vec<CXQuery<String>>,
    origin_query: Option<String>,
    _phantom: PhantomData<TP>,
}

impl<'py, S, TP> PandasDispatcher<'py, S, TP>
where
    S: Source,//S：表示数据源类型，必须实现 Source trait.
    TP: Transport<TSS = S::TypeSystem, TSD = PandasTypeSystem, S = S, D = PandasDestination<'py>>,//TP：表示传输协议类型，必须实现 Transport trait，且其类型系统必须与数据源和 Pandas 目的地兼容.
    <TP as connectorx::typesystem::Transport>::Error: From<ConnectorXPythonError>,
{
    /// Create a new dispatcher by providing a source, a destination and the queries.
    pub fn new<Q>(
        src: S,
        dst: PandasDestination<'py>,
        queries: &[Q],
        origin_query: Option<String>,
    ) -> Self
    where
        for<'a> &'a Q: Into<CXQuery>,
    {
        Self {
            src,
            dst,
            queries: queries.iter().map(Into::into).collect(),
            origin_query,
            _phantom: PhantomData,
        }
    }

    /// Start the data loading process.
    /// //从数据源读取数据并将其写入 Pandas 数据框
    /// py: Python<'py> 类型，表示 Python 解释器的引用.
    /// mut self：表示方法会修改 PandasDispatcher 实例的状态.
    pub fn run(mut self, py: Python<'py>) -> Result<Bound<'py, PyAny>, TP::Error> {
        debug!("Run dispatcher");

        debug!("Prepare");
        ///准备阶段：
        //
        // 确定数据顺序：使用 coordinate 函数确定数据源和目的地的数据顺序，并设置数据源的数据顺序.
        // 设置查询和原始查询：将查询列表和原始查询设置到数据源中
        let dorder = coordinate(S::DATA_ORDERS, PandasDestination::DATA_ORDERS)?;
        self.src.set_data_order(dorder)?;
        self.src.set_queries(self.queries.as_slice());
        self.src.set_origin_query(self.origin_query);

        debug!("Fetching metadata");
        //调用数据源的 fetch_metadata 方法获取元数据.
        // 获取数据源的模式（src_schema）和列名（names）.
        // 将数据源的模式转换为目标模式（dst_schema）.
        self.src.fetch_metadata()?;
        let src_schema = self.src.schema();
        let dst_schema = src_schema
            .iter()
            .map(|&s| TP::convert_typesystem(s))
            .collect::<CXResult<Vec<_>>>()?;
        let names = self.src.names();

        //获取总行数：
        //
        // 如果目的地需要行数，则尝试获取整个结果的行数. 如果无法获取，则手动计算每个分区的行数并求和.
        // 如果目的地不需要行数，则直接设置为 Some(0).
        let mut total_rows = if self.dst.needs_count() {
            // return None if cannot derive total count
            debug!("Try get row rounts for entire result");
            self.src.result_rows()?
        } else {
            debug!("Do not need counts in advance");
            Some(0)
        };
        let mut src_partitions: Vec<S::Partition> = self.src.partition()?;
        if self.dst.needs_count() && total_rows.is_none() {
            debug!("Manually count rows of each partitioned query and sum up");
            // run queries
            src_partitions
                .par_iter_mut()
                .try_for_each(|partition| -> Result<(), S::Error> { partition.result_rows() })?;

            // get number of row of each partition from the source
            let part_rows: Vec<usize> = src_partitions
                .iter()
                .map(|partition| partition.nrows())
                .collect();
            total_rows = Some(part_rows.iter().sum());
        }
        let total_rows = total_rows.ok_or_else(ConnectorXError::CountError)?;

        //根据总行数和列数为 Pandas 目的地分配内存.
        debug!(
            "Allocate destination memory: {}x{}",
            total_rows,
            src_schema.len()
        );
        self.dst
            .allocate_py(py, total_rows, &names, &dst_schema, dorder)?;

        //根据查询数量创建目的地的分区.
        debug!("Create destination partition");
        let dst_partitions = self.dst.partition(self.queries.len())?;

        #[cfg(all(not(feature = "branch"), not(feature = "fptr")))]
        compile_error!("branch or fptr, pick one");

        #[cfg(feature = "branch")]
        let schemas: Vec<_> = src_schema
            .iter()
            .zip_eq(&dst_schema)
            .map(|(&src_ty, &dst_ty)| (src_ty, dst_ty))
            .collect();

        debug!("Start writing");

        //数据写入：
        //
        // 释放 GIL（全局解释器锁），允许多线程执行数据写入操作.
        // 对于每个分区，解析数据并将其写入目的地：
        // 根据数据顺序（行主序或列主序）进行数据写入.
        // 使用不同的特性（branch 或 fptr）来选择处理数据的方式：
        // fptr 特性：使用函数指针来处理数据.
        // branch 特性：使用分支逻辑来处理不同类型的数据.
        // 每个分区写入完成后，调用 finalize 方法完成分区的处理.
        // release GIL
        py.allow_threads(move || -> Result<(), TP::Error> {//调用 allow_threads 方法来释放 Python 的全局解释器锁（GIL），允许 Rust 代码在多线程环境中运行，而不受 GIL 的限制. 这使得数据处理可以并行进行，提高数据加载的效率.
            // parse and write
            //dst_partitions.into_par_iter().zip_eq(src_partitions).enumerate()：将目的地分区和数据源分区进行并行迭代，并附加一个索引（enumerate）.
            // 这种并行迭代使得每个分区可以独立地进行数据处理，进一步提高性能.
            dst_partitions
                .into_par_iter()
                .zip_eq(src_partitions)
                .enumerate()
                .try_for_each(|(i, (mut dst, mut src))| -> Result<(), TP::Error> {
                    #[cfg(feature = "fptr")]
                    let f: Vec<_> = src_schema
                        .iter()
                        .zip_eq(&dst_schema)
                        .map(|(&src_ty, &dst_ty)| TP::processor(src_ty, dst_ty))
                        .collect::<CXResult<Vec<_>>>()?;

                    //读取数据
                    let mut parser = src.parser()?;//为每个数据源分区创建一个解析器，用于从数据源中读取数据.

                    //根据数据顺序（dorder）进行不同的数据处理
                    match dorder {
                        //行主序（RowMajor）：
                        // let (n, is_last) = parser.fetch_next()?;：从解析器中读取下一个数据块，获取数据块的大小 n 和是否是最后一个数据块的标志 is_last.
                        // dst.aquire_row(n)?;：为目的地分配空间以存储 n 行数据.
                        // 对于每一行数据，遍历所有列，根据配置（fptr 或 branch 特性）调用相应的处理函数来处理数据并写入目的地.
                        // 如果 is_last 为 true，则结束循环.
                        DataOrder::RowMajor => loop {
                            let (n, is_last) = parser.fetch_next()?;
                            dst.aquire_row(n)?;
                            for _ in 0..n {
                                #[allow(clippy::needless_range_loop)]
                                for col in 0..dst.ncols() {
                                    #[cfg(feature = "fptr")]
                                    f[col](&mut parser, &mut dst)?;

                                    #[cfg(feature = "branch")]
                                    {
                                        let (s1, s2) = schemas[col];
                                        TP::process(s1, s2, &mut parser, &mut dst)?;
                                    }
                                }
                            }
                            if is_last {
                                break;
                            }
                        },
                        //列主序（ColumnMajor）：
                        // 类似于行主序，但数据处理的顺序是按列进行的.
                        // 对于每一列数据，遍历所有行，处理数据并写入目的地.
                        DataOrder::ColumnMajor => loop {
                            let (n, is_last) = parser.fetch_next()?;
                            dst.aquire_row(n)?;
                            #[allow(clippy::needless_range_loop)]
                            for col in 0..dst.ncols() {
                                for _ in 0..n {
                                    #[cfg(feature = "fptr")]
                                    f[col](&mut parser, &mut dst)?;
                                    #[cfg(feature = "branch")]
                                    {
                                        let (s1, s2) = schemas[col];
                                        TP::process(s1, s2, &mut parser, &mut dst)?;
                                    }
                                }
                            }
                            if is_last {
                                break;
                            }
                        },
                    }

                    debug!("Finalize partition {}", i);
                    dst.finalize()?;
                    debug!("Partition {} finished", i);
                    Ok(())
                })?;
            Ok(())
        })?;
        debug!("Writing finished");

        Ok(self.dst.result(py).unwrap())
    }

    /// Only fetch the metadata (header) of the destination.
    pub fn get_meta(mut self, py: Python<'py>) -> Result<Bound<'py, PyAny>, TP::Error> {
        let dorder = coordinate(S::DATA_ORDERS, PandasDestination::DATA_ORDERS)?;
        self.src.set_data_order(dorder)?;
        self.src.set_queries(self.queries.as_slice());
        self.src.set_origin_query(self.origin_query.clone());
        self.src.fetch_metadata()?;
        let src_schema = self.src.schema();
        let dst_schema = src_schema
            .iter()
            .map(|&s| TP::convert_typesystem(s))
            .collect::<CXResult<Vec<_>>>()?;
        let names = self.src.names();
        self.dst.allocate_py(py, 0, &names, &dst_schema, dorder)?;
        Ok(self.dst.result(py).unwrap())
    }
}
