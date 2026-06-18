# 开发计划：pytdx 扩展接口（tick / 分时 / 财务 / F10 / 指数 / 枚举）

> 在已有 `download_daily / download_minute / download_xdxr / snapshot` 之外，补齐 pytdx 暴露但尚未封装的接口。
> 验证基线见 `tests/test_pytdx_extended_integration.py`（7 passed）。

## 已确认的接口可达性（HQ / ExHQ）

| pytdx 方法 | 含义 | 状态 |
|---|---|---|
| `get_history_transaction_data` | 历史逐笔成交 (tick) | ✅ 深度 ≥ 1 年 |
| `get_transaction_data` | 当日逐笔 | ✅（盘后完整，盘中可能 vol=0）|
| `get_history_minute_time_data` | 历史分时 (240 点/日) | ✅ |
| `get_minute_time_data` | 当日分时 | ✅ |
| `get_finance_info` | **股本结构**（非利润表）| ✅ |
| `get_company_info_category` + `get_company_info_content` | F10 公司资料（文本）| ✅ |
| `get_index_bars` | 指数 K 线（含涨跌家数）| ✅ |
| `get_security_count` / `get_security_list` | 证券枚举 | ✅ (SZ 23485) |
| ExHQ `get_instrument_count` / `get_instrument_info` | 扩展行情枚举 | ✅ (131971) |
| ExHQ `get_markets` | 市场列表 | ⚠️ 本服务器返回 None |

## 关键设计决策

1. **逐笔存储 → 独立的按日分区**：`data/tdx_transactions/date=YYYYMMDD/ts_code=<...>/data.parquet`
   （不复用 per-symbol 覆盖写；单股单日上万条，按日分区便于按日扫描与回填）
2. **财务报表（EPS/营收/净利润）→ 解析 F10 文本**；`get_finance_info` 仅作「股本结构」单独管道
3. **F10 / 枚举提前**到队列前段

## 数据坑（实现时务必处理）

- 当日 vs 历史逐笔 schema 不同：历史 `time/price/vol/buyorsell`，当日多 `num` → extractor 分两路统一
- `buyorsell`：0=买盘 / 1=卖盘，实测还存在其它取值（如 8）→ Phase 2 枚举语义并加 `buyorsell_label`
- 逐笔含零量行（撤单/中性）→ 保留但标记，不丢弃
- 当日接口盘前/盘中不完整 → 测试与下载一律用「最近一个已完成交易日」
- F10 `get_company_info_content` 第 3 参是 `filename`（如 `000001.txt`），`start/length` 是该文件内字节偏移；**不是**显示名
- F10 文本格式随数据源变化 → Phase 3 先做文本采样 spike 再定型解析器
- `get_finance_info` ≠ 利润表（只有股本结构 + IPO/行业/省份）

## 分层落地规则（每个 Phase 统一）

`connectors` 加 `fetch_*_payload` → `extractors` 加 `tdx_*.py`（带离线单测 fixture）→ `materializers` 落盘 → `jobs/*_job.py` → `tdx_client.TdxDownloader` 加方法 → 集成测试。
**完成门禁**：离线单测绿（`-m "not integration"`）+ 集成测试绿。

---

## Phase 0 — 逐笔深度探查 ✅ 已完成
深度 ≥ 1 年（000001.SZ / 600000.SH 全 250 个交易日均有数据）。逐笔管道可行。

## Phase 1 — 证券枚举（提前）✅ 已完成
- connectors：`fetch_security_count_payload`、`fetch_security_list_payload(market, start, count=1000)`（按 1000 翻页）
- extractor `tdx_security_list.py`：`code/name/market/ts_code/pre_close/decimal_point/volunit`
- materializer：快照按日分区 `data/security_list/market=<SZ|SH>/date=YYYYMMDD/data.parquet`
- job `security_list_job.py`；`TdxDownloader.download_security_list(market)`
- ExHQ：`fetch_instrument_info_payload` 翻页 → `data/instrument_list/`；`get_markets` 不可靠 → 跳过/兜底
- **意义**：建立全市场代码表，为后续全市场批扫铺路

## Phase 2 — 逐笔成交 tick ⭐（按日分区）✅ 已完成
- connectors：`fetch_history_transaction_payload(market, code, start, count, date)` 带 `start` 翻页循环；`fetch_transaction_payload`（当日）
- extractor `tdx_transactions.py`：统一两路 schema → `trade_time/price/vol/num(可空)/buyorsell/buyorsell_label/ts_code/trade_date`；枚举 `buyorsell` 全部取值；零量行加标记
- materializer：**新增 `write_raw_by_date_symbol`** → `data/tdx_transactions/date=YYYYMMDD/ts_code=<...>/data.parquet`
- job `transaction_job.py`；`TdxDownloader.download_tick(code, date)` / `download_tick_today(code)`
- 子任务：全量翻页策略（从收盘往前直到 start 越界返回空）

## Phase 3 — F10 公司资料 + 财务报表（文本解析）✅ 已完成（主要财务指标）
- connectors：`fetch_company_info_category_payload`、`fetch_company_info_content_payload(filename, start, length)`
- extractor `tdx_company_info.py`：拉分类 → 取每段文本
- **先做文本采样 spike**：dump 3~5 只票的 F10 全文，定位含财务数据的段落（如「财务分析」「财务概况」）
- 财务解析：EPS / BPS / 主营收入 / 净利润 / ROE / 毛利率 等字段 → `data/company_finance/ts_code=<...>/date=YYYYMMDD/data.parquet`
- 原始文本留存 `data/company_info_raw/` 以便改解析器时重跑
- job `company_info_job.py`；`TdxDownloader.download_company_info(code)` / `download_finance_statements(code)`
- 单测用 canned 文本 fixture（不打网）

## Phase 4 — 分时数据 ✅ 已完成
- connectors：`fetch_minute_time_payload`（当日）、`fetch_history_minute_time_payload(market, code, date)`
- extractor `tdx_minute_time.py`：240 点 → `trade_time(09:30..15:00)/price/vol/ts_code/trade_date`
- materializer：per-symbol 按日 `data/minute_time/ts_code=<...>/date=YYYYMMDD/data.parquet`
- job `minute_time_job.py`；`TdxDownloader.download_minute_time(code, date)`

## Phase 5 — 财务股本结构（get_finance_info）✅ 已完成
- connectors：`fetch_finance_payload(market, code)`
- extractor `tdx_finance.py`：`ts_code/zongguben/liutongguben/guojiagu/farengu/bgu/ipo_date/industry/province/updated_date`
- materializer：`data/finance_capital/ts_code=<...>/date=YYYYMMDD/data.parquet`
- job `finance_capital_job.py`；`TdxDownloader.download_finance_capital(code)`
- ⚠️ 与 Phase 3（报表）、现有 tushare `financial_job.py` 三者命名区分清楚

## Phase 6 — 指数 K 线 ✅ 已完成
- connectors：`fetch_index_bars_payload(category, market, code, start, count)` 带翻页
- extractor `tdx_index_bars.py`：复用 bars 形状 + `up_count/down_count`
- materializer：per-symbol `data/index_daily/ts_code=<如 000001.SH>/...`
- code_mapping：指数代码解析（000001.SH 上证指数、399001.SZ 深证成指 …）
- job `index_bars_job.py`；`TdxDownloader.download_index(code)`

---

## 待定 / 开放项
- `buyorsell` 非常规取值（8 等）的确切语义 → Phase 2 枚举
- F10 各段落实际文本结构 → Phase 3 spike 定型
- 逐笔全量翻页的性能/断点续传策略 → Phase 2
