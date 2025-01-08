from __future__ import annotations

from typing import overload, Literal, Any, TypeAlias, TypedDict
import numpy as np

#类型别名
#_ArrowArrayPtr 和 _ArrowSchemaPtr：这两个类型别名都是整数类型（int），用于表示 Arrow 数组和模式的指针.
#_Header：类型别名表示一个字符串，用于表示数据的头部信息（如列名）.
_ArrowArrayPtr: TypeAlias = int
_ArrowSchemaPtr: TypeAlias = int
_Header: TypeAlias = str

# 类定义
# PandasBlockInfo：一个简单的类，包含两个字段：
# cids：一个整数列表，表示块的列 ID.
# dt：一个整数，表示块的数据类型.
class PandasBlockInfo:
    cids: list[int]
    dt: int

# 字典类型
# _DataframeInfos：一个 TypedDict 类型，定义了一个包含数据、头部信息和块信息的字典：
# data：一个列表，包含元组或 NumPy 数组，表示数据块.
# headers：一个字符串列表，表示数据的头部信息（如列名）.
# block_infos：一个 PandasBlockInfo 对象列表，表示每个数据块的信息.
class _DataframeInfos(TypedDict):
    data: list[tuple[np.ndarray, ...] | np.ndarray]
    headers: list[_Header]
    block_infos: list[PandasBlockInfo]

_ArrowInfos = tuple[list[_Header], list[list[tuple[_ArrowArrayPtr, _ArrowSchemaPtr]]]]


#  read_sql 函数的重载签名：
# 第一个重载签名：当 return_type 为 "pandas" 时，返回 _DataframeInfos 类型的数据. 这种情况用于返回 Pandas 数据框的信息.
# 第二个重载签名：当 return_type 为 "arrow" 或 "arrow2" 时，返回 _ArrowInfos 类型的数据. 这种情况用于返回 Arrow 格式的数据.
@overload
def read_sql(
    conn: str,
    return_type: Literal["pandas"],
    protocol: str | None,
    queries: list[str] | None,
    partition_query: dict[str, Any] | None,
) -> _DataframeInfos: ...
@overload
def read_sql(
    conn: str,
    return_type: Literal["arrow", "arrow2"],
    protocol: str | None,
    queries: list[str] | None,
    partition_query: dict[str, Any] | None,
) -> _ArrowInfos: ...
def partition_sql(conn: str, partition_query: dict[str, Any]) -> list[str]: ...
def read_sql2(sql: str, db_map: dict[str, str]) -> _ArrowInfos: ...
def get_meta(
    conn: str,
    protocol: Literal["csv", "binary", "cursor", "simple", "text"] | None,
    query: str,
) -> _DataframeInfos: ...
