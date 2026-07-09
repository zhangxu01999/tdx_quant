"""DuckDB 按需查询层，直接读取按股票分区的 Parquet 行情。"""

__all__ = ["DuckDBMarketStore"]


def __getattr__(name: str):
    """延迟导入，避免用 ``python -m`` 执行子模块时被提前加载。"""

    if name == "DuckDBMarketStore":
        from .duckdb_store import DuckDBMarketStore

        return DuckDBMarketStore
    raise AttributeError(name)
