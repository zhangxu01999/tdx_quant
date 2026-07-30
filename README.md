# tdx_quant — 通达信(pytdx)数据获取 · 指标计算 · 选股

基于 **pytdx** 的 A 股数据管道：下载 → 落盘(parquet) → 计算技术指标 → 条件选股。
仅依赖 pytdx，不接 tushare / baostock；复用 `scripts/data_pipeline/` 已有的多主机轮询连接层。

---

## 目录结构

```text
scripts/data_pipeline/
├── tdx_client.py                 # 高层下载封装 TdxDownloader（全部下载入口）
├── connectors/
│   └── pytdx_client.py           # create_hq_api / connected_session / fetch_*_payload 原语
├── extractors/                   # payload → DataFrame，每个接口一个 tdx_*.py
│   ├── tdx_bars.py / tdx_xdxr.py # 日/分钟 K 线 + 除权除息
│   ├── tdx_index_bars.py         # 指数 K 线（含涨跌家数）
│   ├── tdx_transactions.py       # 分笔成交
│   ├── tdx_minute_time.py        # 分时（每分钟一点）
│   ├── tdx_finance.py            # 股本结构
│   ├── tdx_company_info.py       # F10 财务分析文本解析
│   └── tdx_security_list.py      # 全市场证券枚举
├── jobs/                         # fetch + normalize + 落盘 的可复用任务
│   └── *_job.py                  # daily / minute / transaction / minute_time / finance_capital / company_info / security_list ...
├── materializers/symbol_writer.py# write_by_symbol：按 ts_code 分区写 parquet
├── indicators/
│   ├── trend.py                  # MA / EMA / MACD
│   ├── momentum.py               # RSI / KDJ
│   ├── volatility.py             # BOLL / ATR
│   ├── volume.py                 # VOL_MA / 量比 / 换手率近似
│   └── core.py                   # INDICATORS 注册表 + compute_all
└── screener/
    ├── conditions.py             # 金叉/突破/超卖 等声明式条件 + CONDITIONS
    └── run_screener.py           # screen() 批量入口 + 命令行

scripts/tdx_mcp/                  # 通达信 MCP（HTTP/SSE 实时数据，与 pytdx 互补）
├── tdx_client.py                 # 基础客户端 TdxMcpClient / TdxQueryResult
├── tdx_stock_analyzer.py         # 个股四维诊断（行情/技术/财务/资金）
├── tdx_market_daily.py           # 每日市场概览（7 板块并发）
├── tdx_concept_board.py          # 概念板块成分股 / 热度 / 跨概念对比
├── tdx_limit_up.py               # 涨停板 / 连板梯队 / 概念集中度
└── tdx_data_enricher.py          # 批量增补 → data/tdx_*.json（概念/北向/机构/评级/筹码）
```

---

## 环境依赖

需要 `pytdx`、`pandas`、`pyarrow`、`numpy`（任一满足即可）：

```bash
pip install pytdx pandas pyarrow numpy
```

> 第 5 节「通达信 MCP」的脚本走 HTTP，额外需要 `httpx`：`pip install httpx`。

所有命令在项目根目录 `/Users/henrylin/trae_space/tdx_quant` 下运行。

---

## 1. 下载：`tdx_client.TdxDownloader`

```python
from pathlib import Path
from scripts.data_pipeline.tdx_client import TdxDownloader

dl = TdxDownloader(Path("data"))

daily   = dl.download_daily("000001")          # 日K全历史(自动翻页),落盘并返回
minute  = dl.download_minute("000001", freq=5) # 5分钟线,带 trade_time 列
xdxr    = dl.download_xdxr("000001")           # 除权除息
snap    = dl.snapshot("000001")                # 实时快照(hq); snapshot("AAPL") 走 exhq
```

- 传入 6 位代码即可，内部按 `infer_hq_market` 判定沪(1)/深(0)，`ts_code` 形如 `000001.SZ`。
- `download_daily/minute/xdxr` 仅支持沪深主板；非主板代码直接 `ValueError`。
- 拉空 / 连接失败 → 直接 raise，不返回空表。

### 股票池批量下载与每日增量更新

编辑 `configs/daily-sync.json`，然后运行：

```bash
python -m scripts.data_pipeline.batch_daily --config configs/daily-sync.json
```

首次下载每只股票最近 `history_bars` 根日线；后续运行只重新获取最近 `refresh_bars` 根，
与本地历史合并并按时间去重。任务支持并发、逐股重试、断点续跑和 JSON 结果报告，单只股票失败
不会覆盖其他股票的数据。批量任务会为每个工作线程建立一条长连接并循环复用，而不是逐只股票
重新连接；多个连接会轮换使用默认 TDX 节点。默认 `workers=8`，不要盲目继续增大并发。
为避免 PyCharm 渲染数千行日志拖慢任务，默认每 `progress_every=20` 只输出一次进度，失败仍会
立即输出，完整逐股结果保存在 `data/daily-sync-report.json`。
PyCharm 可直接运行 `tdx-quant：股票池日线同步`。

当前默认 `universe` 为 `all-a-shares`，会从 `data/security_list` 快照中解析全部沪深 A 股。
如果只是小批量调试，可改回 `configured` 并手写 `symbols`，或保留全市场模式但设置
`max_symbols` 限制数量：

```json
{
  "universe": "all-a-shares",
  "symbols": [],
  "max_symbols": 100
}
```

确认网络和磁盘无误后，把 `max_symbols` 改为 `null` 或删除该字段即可同步证券列表中的全部沪深 A 股。
`all-a-shares` 依赖已经下载的 `data/security_list` 快照；证券列表存在不等于日线已经落盘，
回测端只会扫描 `data/daily/ts_code=*/` 中实际下载成功的股票。

### 盘中观察池与模拟交易

先运行 `quant-engine` 的激进短线一键流水线。流水线会在每次结果目录生成
`intraday-watchlist.json`，其中包含最后一个日频决策的观察候选、实际目标和当前持仓。
随后在交易日上午运行：

```bash
python -m scripts.data_pipeline.intraday_paper_cli \
  --config configs/intraday-paper.json
```

PyCharm 可直接选择 `tdx-quant：盘中观察池模拟交易`。进程只轮询观察池，不扫描全市场：

1. 09:30～11:30、13:00～15:00 复用一条 pytdx 长连接，默认每 5 秒分批读取实时快照；
2. 原始快照和本地聚合的 1 分钟 K 线写入 `data/intraday-paper.duckdb`；
3. 计算按交易进度校正的量比、累计成交均价线、过去 20 分钟突破、炸板和回封；
4. 满足“放量 + 分时突破 + 站上均价线”或“炸板后重新回封”时生成模拟买单；
5. 止损、移动止损或明显跌破均价线时生成模拟卖单；
6. 信号不会使用同一个报价成交，订单等待下一次快照；模拟账户遵守整手、费用、滑点、
   最大持仓数、T+1，以及涨跌停无对手盘时不可成交；
7. 15:05 用 pytdx 最新日 K 对账收盘价、成交量和成交额，记录收盘权益后退出。

这是一套本地模拟交易系统，不连接券商，也不会发送真实委托。首次运行前必须先更新日线并
运行日频研究流水线；配置默认拒绝使用超过 5 个自然日的旧观察池。市场风险关闭时只监控已有
持仓，不会把“仅观察”候选转成买单。

常用检查命令：

```bash
# 非交易时间只测试一次 pytdx 连接和落盘，不持续运行
python -m scripts.data_pipeline.intraday_paper_cli \
  --config configs/intraday-paper.json --once --ignore-session

# 不联网，查看数据库各表行数、现金和持仓
python -m scripts.data_pipeline.intraday_paper_cli \
  --config configs/intraday-paper.json --status

# 日线稍晚发布时，手工重跑指定日期的收盘对账
python -m scripts.data_pipeline.intraday_paper_cli \
  --config configs/intraday-paper.json --reconcile-only --trade-date 2026-07-30
```

主要配置都在 `configs/intraday-paper.json`：观察池来源、手工补充代码、轮询间隔、量比和
突破阈值、止损、账户资金、持仓数量、费用与滑点均可直接修改。DuckDB 主要表如下：

| 表 | 用途 |
|---|---|
| `realtime_snapshots` | 每次 pytdx 原始快照及标准字段 |
| `minute_bars_1m` | 按累计成交量差分聚合的 1 分钟 OHLCV |
| `intraday_signals` | 量比、均价线、突破、炸板、回封和买卖原因 |
| `paper_orders` / `paper_trades` | 模拟委托和下一快照成交 |
| `paper_positions` / `paper_account_snapshots` | 模拟持仓和账户权益 |
| `daily_reconciliation` | 快照聚合与盘后日 K 的差异 |

### 短线日频增强特征

短线策略除了 OHLCV，还需要成交额、换手率、流通市值、涨跌停和炸板等字段。日线同步完成后运行：

```bash
python -m scripts.data_pipeline.batch_finance_capital --config configs/finance-capital-sync.json
```

该任务会从 `data/security_list` 解析全市场 A 股，逐只下载 pytdx HQ 的股本结构快照并写入
`data/finance_capital`。PyCharm 可直接运行 `tdx-quant：全市场股本结构同步`。

随后生成短线增强特征：

```bash
python -m scripts.data_pipeline.short_term_features --data-root data
```

脚本会读取：

- `data/daily`：价格、成交量、成交额；
- `data/finance_capital`：流通股本，若某只股票暂缺该数据，则换手率和流通市值为空。

并生成：

```text
data/short_term_daily/ts_code=<股票代码>/data.parquet
```

当前输出字段包括 `amount`、`turnover_rate`、`float_market_cap`、`limit_up`、`limit_down`、
`hit_limit_up`、`hit_limit_down`、`bomb_limit_up`、`volume_ratio` 和 `amount_growth`。涨跌停状态先按
主板 10%、创业板/科创板 20%、北交所 30% 从 OHLC 推导；ST、复牌首日和新股等特殊规则后续再接更精确数据源。

### 扩展接口（均在原 4 个接口之外补充）

除上面的 `daily / minute / xdxr / snapshot`，`TdxDownloader` 另封装了 6 类接口（**均仅支持沪深主板 6 位代码**，非主板直接 `ValueError`）：

```python
sec   = dl.download_security_list(1)               # 全市场枚举快照(0=SZ / 1=SH)
idx   = dl.download_index("000001", market=1)      # 指数 K 线；market 必须显式传(000001=上证指数,SH)
tick  = dl.download_tick("000001", 20240610)       # 指定日分笔成交(YYYYMMDD 或 YYYY-MM-DD)
tick0 = dl.download_tick_today("000001")           # 当日分笔成交(盘中可能不完整)
mt    = dl.download_minute_time("000001", 20240610)# 指定日分时(每个交易日 ≈ 240 点)
mt0   = dl.download_minute_time_today("000001")    # 当日分时
fin   = dl.download_company_finance("000001")      # F10 主要财务指标(long 格式)
cap   = dl.download_finance_capital("000001")      # 股本结构快照(单行)
```

| 方法 | 含义 | 返回 DataFrame 关键列 |
|------|------|------------------------|
| `download_security_list(market)` | 全市场证券枚举（每日快照） | `ts_code, code, name, pre_close` |
| `download_index(code, *, market, max_bars)` | 指数日 K 线 | `trade_date, open/high/low/close, vol, amount, up_count, down_count` |
| `download_tick(code, date)` / `download_tick_today(code)` | 分笔成交（按日分区） | `time, price, vol, buyorsell, buyorsell_label` |
| `download_minute_time(code, date)` / `..._today(code)` | 分时线（每分钟一点） | `minute_idx(0基序), price, vol` |
| `download_company_finance(code)` | F10「主要财务指标」解析 | `metric, period, value_raw, value_num` |
| `download_finance_capital(code)` | 股本结构（`get_finance_info` 快照，非利润表） | `zongguben, liutongguben, ipo_date, industry, province` |

- `download_index` 的 `market` **必须显式传入**：指数代码不遵循个股前缀规则（上证指数 `000001` 属沪市 market=1，与深市平安银行 `000001.SZ` 同码不同市）。
- `download_company_finance` 另把 F10 原文落盘到 `data/company_info_raw/`，便于重解析；`value_num` 已把 `亿/万` 归一到元、文本/`-` 置 NaN。
- `download_tick` / `download_minute_time` 的当日版本（`*_today`）盘前/盘中数据可能不完整，盘后才齐全。

### 落盘格式

按数据特性分三种分区方式，统一写 parquet：

| 接口 | 域 `<domain>` | 分区布局 |
|------|---------------|----------|
| `download_daily` | `daily` | `ts_code=<...>/data.parquet`（覆盖写，全历史） |
| `download_minute` | `minute_5m\|15m\|30m\|60m` | `ts_code=<...>/data.parquet` |
| `download_xdxr` | `xdxr` | `ts_code=<...>/data.parquet` |
| `download_index` | `index_daily` | `ts_code=<...>/data.parquet` |
| `download_tick` / `download_tick_today` | `tdx_transactions` | `date=<YYYYMMDD>/ts_code=<...>/data.parquet`（按日分区，便于按日扫描/回填） |
| `download_minute_time` / `..._today` | `minute_time` | `date=<YYYYMMDD>/ts_code=<...>/data.parquet` |
| `download_company_finance` | `company_finance` | `ts_code=<...>/data.parquet`（+ 原文 `company_info_raw/`） |
| `download_finance_capital` | `finance_capital` | `ts_code=<...>/data.parquet` |
| `download_security_list` | `security_list` | `market=<SZ\|SH>/date=<YYYYMMDD>/`（每日快照） |
| `short_term_features` | `short_term_daily` | `ts_code=<...>/data.parquet`（从日线和股本结构派生） |

`ts_code` / `date` / `market` 等分区键只存在路径里（文件内不重复存），读时由 hive 分区还原，`pd.read_parquet('data/daily')` 即可一次读回该 domain 下全部股票。`download_tick` / `download_minute_time` / `download_finance_capital` / `download_company_finance` 返回时会把这些键重新挂回 DataFrame 列上。

---

## 2. 指标计算：`indicators.compute_all`

纯 pandas 实现，输入含 `close/high/low/vol/amount` 且按时间升序的 DataFrame：

```python
from scripts.data_pipeline.indicators import compute_all

ind = compute_all(daily, timeframe="daily", shares=1e9)  # shares 可选,用于换手率
# ind 在副本上附加全部指标列
```

| 类别 | 函数 | 产出列 |
|------|------|--------|
| 趋势 | `calc_ma` / `calc_ema` / `calc_macd` | `MA5/10/20/60`、`EMA5/10/20/60`、`DIF/DEA/MACD` |
| 动量 | `calc_rsi` / `calc_kdj` | `RSI6/12/24`、`K/D/J` |
| 波动 | `calc_boll` / `calc_atr` | `BOLL_MB/BOLL_UP/BOLL_DN`、`ATR` |
| 量能 | `calc_vol_ma` / `calc_volume_ratio` / `calc_turnover` | `VOL_MA5/10`、`VOL_RATIO`、`TURNOVER_RATE` |

- `timeframe="minute"` 使用更短周期（去掉 MA60 / RSI24）。
- 通达信约定：EMA 全程 `adjust=False`、MACD 柱 `(DIF-DEA)*2`、BOLL 总体标准差(`ddof=0`)、RSI/ATR Wilder 平滑。
- 暖机行（均线头部等）为 NaN 属正常；`compute_all` 返回副本，不修改入参。

---

## 3. 选股：`screener`

**多周期**：每只票在 日线 + 5/15/30/60 分钟线 上各跑一遍条件；冷启动时每个周期下载约 `max_bars` 根 K 线（默认 200）并落盘，之后读缓存。

### 程序调用

```python
from scripts.data_pipeline.screener.run_screener import screen
from scripts.data_pipeline.screener.conditions import golden_cross, rsi_oversold

result = screen(
    ["000001", "600000", "000002"],
    [golden_cross, rsi_oversold],
    data_root="data",
    max_bars=200,          # 每个周期最多取的 K 线根数，特殊情况调大
)
# 列: ts_code, timeframe, close, hit_count, matched, latest_trade_date
# 每个 (股票, 周期) 一行；按 hit_count 降序
```

`screen` 对每只票 × 每个周期：优先读 `data_root/<domain>/ts_code=<>/` 已落盘 parquet（`domain` ∈ daily / minute_5m|15m|30m|60m），无则下载 → `compute_all` → 逐条件取该周期最新一根 K 线的布尔值。单个 (股票,周期) 异常会被跳过（stderr 打 `WARNING: skip <code> <tf>`），不影响整批。

### 内置条件（每个周期评估最新一根 K 线）

| 名称 | 含义 |
|------|------|
| `golden_cross` | MACD 金叉（当根 DIF 上穿 DEA） |
| `kdj_golden_cross` | KDJ 金叉（K 上穿 D） |
| `volume_breakout(df, n=5, k=2)` | 放量突破（量比>k 且 收盘>MA20） |
| `rsi_oversold(df, threshold=30)` | RSI6 超卖 |
| `near_boll_lower` | 收盘触及或跌破布林下轨 |

### 命令行

```bash
# 行内代码（默认每周期取 200 根）
python -m scripts.data_pipeline.screener.run_screener \
  --codes 000001,600000,000002 \
  --conditions golden_cross,rsi_oversold,near_boll_lower \
  --data-root data \
  --max-bars 200          # 可选: 特殊情况调大

# 或从 JSON 文件读代码清单 (--codes-file watchlist.json,内容为代码字符串数组)
python -m scripts.data_pipeline.screener.run_screener \
  --codes-file watchlist.json --conditions golden_cross,volume_breakout \
  --output result.csv        # 可选: 同时写出 CSV
```

---

## 4. 前端可视化：`frontend/`（A股量化数据终端）

数据终端默认通过本地只读 API 按需查询 DuckDB/Parquet。页面顶部的全局股票选择器支持按代码
或名称搜索；选择股票后，日 K、多周期分钟线、逐笔成交和公司基本面同步切换。日线会动态计算
MA、BOLL、MACD、RSI、KDJ，并合并成交额、换手率、流通市值和股本快照。

```bash
python -m frontend.server --data-root data --port 8765 --bind 127.0.0.1
# 浏览器打开 http://127.0.0.1:8765/
```

股票选择器采用分页懒加载，每页读取 100 只，滚动到下拉框底部会自动加载下一页，因此可遍历
证券快照内的全部股票；仍可直接输入六位代码或名称进行精确搜索。

- PyCharm 可直接运行 `tdx-quant：启动数据终端`，启动项已经切换到上述动态服务。
- `/api/symbols?q=银行`：按代码或名称搜索证券；兼容通达信名称中的半角/全角空格。
- `/api/market/overview?limit=240`：返回沪深指数、涨跌家数和最新证券数量。
- `/api/stocks/600372.SH?limit=800`：按需返回单股行情、技术指标和短线字段。
- `/api/stocks/000001.SZ/minute`、`ticks`、`fundamentals`：按当前股票返回分钟、逐笔和基本面。
- `data_export.py` 与旧的 5 个 JSON 仅保留为离线快照工具，动态终端不再依赖它们。
- 页面仍使用本地 ECharts，无 CDN 依赖；服务只监听 `127.0.0.1`，不对外网开放。
- 某只股票尚未同步分钟、逐笔或基本面时，对应页面会明确提示并清除旧股票内容，不伪造数据。

### 视图 ↔ 数据来源

每个视图消费的数据域（对应上面的下载接口）：

| 视图 | 动态接口 | 消费数据域（下载接口） |
|------|----------|------------------------|
| 1. 市场概览 | `/api/market/overview` | `index_daily`（`download_index`）+ `security_list`（`download_security_list`） |
| 2. K 线主图 | `/api/stocks/{代码}` | `daily` + `short_term_daily` + `finance_capital` |
| 3. 多周期分时 | `/api/stocks/{代码}/minute` | `minute_5m/15m/30m/60m`（`download_minute`） |
| 4. 逐笔成交 | `/api/stocks/{代码}/ticks` | `tdx_transactions`（`download_tick`）+ `minute_time`（`download_minute_time`） |
| 5. 公司基本面 | `/api/stocks/{代码}/fundamentals` | `company_finance`（`download_company_finance`）+ `finance_capital`（`download_finance_capital`）+ `company_info_raw` |

> 仅在手工运行旧版 `data_export.py` 离线快照工具时，K 线快照才读取
> `data/000001.SZ_indicators.parquet`。动态数据终端会直接读取对应日线并即时计算指标，
> 不要求提前为每只股票生成指标文件。若要刷新旧快照可运行：
>
> ```bash
> python3 -c "
> from scripts.data_pipeline.tdx_client import TdxDownloader
> from scripts.data_pipeline.indicators import compute_all
> df = TdxDownloader('data').download_daily('000001')   # 或读已有 data/daily/ts_code=000001.SZ/
> compute_all(df).to_parquet('data/000001.SZ_indicators.parquet', index=False)
> "
> ```

### 5 个视图（涨=红/跌=绿，A股配色）

1. **市场概览** — 沪深指数卡片 + 涨跌家数（市场宽度）+ 指数双轴走势
2. **K线主图** — 日线 K线 + MA5/10/20 + 布林带 + 成交量 + MACD/RSI/KDJ（联动缩放）
3. **多周期分时** — 股票 × {5/15/30/60 分钟} 可切换 K线
4. **逐笔成交** — 买卖盘分布 + 分时价 + 分钟资金流向（主买/主卖）
5. **公司基本面** — 财务指标多期趋势 + 股本结构 + F10 公司资料

支持锚点直达：`/#kline`、`/#ticks` 等。

---

## 5. 通达信 MCP（实时概念/资金/涨停数据）

通达信 MCP（问小达，`https://mcp.tdx.com.cn:3001/mcp`）是 HTTP/SSE 自然语言数据接口，与上面的 **pytdx 历史管道互补**：

| 数据维度 | pytdx（1~4 节） | 通达信 MCP（本节） |
|----------|:---------------:|:------------------:|
| K 线 / 分笔 / 财务快照 | ✅ 历史全量 | — |
| 概念板块 / 板块成分股 | ❌ | ✅ 实时 |
| 封单金额 / 封成比 / 涨停原因 | ❌ | ✅ 盘中 |
| 主力 / 超大单资金流 | ❌ | ✅ 盘中 |
| 北向资金 / 机构基金持仓 / 分析师评级 / 筹码分布 | ❌ | ✅ |

> MCP 走 HTTP，需联网 + `TDX_API_KEY`；与 pytdx 二进制协议完全独立，互不依赖。

### 环境准备

```bash
pip install httpx
export TDX_API_KEY=TDX-your-api-key   # 必填
```

**密钥只从环境变量读取，仓库内不含任何硬编码 key**（审计已确认：6 个脚本一律 `os.getenv("TDX_API_KEY", "")`，文档里只有 `TDX-your-api-key` 等占位符）。传入方式：

- 命令行脚本：`--api-key`（不传则回退到环境变量 `TDX_API_KEY`）
- 直接调用：`TdxMcpClient(api_key=...)`（不传则读环境变量）

三者皆空时构造即抛 `ValueError`，**不发任何请求**。

| 安全约定 | 说明 |
|----------|------|
| 不入仓 | 真实 key 只放环境变量，代码/文档里无任何明文 |
| `.mcp.json` 已 ignore | Claude Code 工具模式配置会带 key，`.gitignore` 已覆盖，勿提交 |
| 失败快 | 缺 key 在构造期就报错，不会带空 header 去打 MCP |

### 基础客户端：`TdxMcpClient`

```python
from scripts.tdx_mcp import TdxMcpClient

client = TdxMcpClient()                      # 读环境变量 TDX_API_KEY
result = client.query("人工智能概念板块成分股 今日涨跌幅", size=50)
print(result.ok(), result.total)
print(result.to_dicts())                     # list[dict]，字段名 → 值

# 自动翻页（合并多页，最多 max_pages 页）
result_all = client.query_all("DeepSeek概念板块成分股", page_size=50, max_pages=20)
```

- `question` 为自然语言；`range`：`AG`(A股,默认) / `HK-GP`(港股) / `JJ`(基金) / `ZS`(指数)。
- 字段名常带日期后缀（如 `主力净流入(万元)\n2026.06.210#`），`to_dicts()` 后做子串模糊匹配（脚本里的 `_find_field`）。

### 命令行脚本（均在 `scripts/tdx_mcp/`）

项目惯例用 `-m` 运行（也支持 `python scripts/tdx_mcp/<name>.py` 直接跑）：

| 脚本 | 用途 | 常用参数 |
|------|------|----------|
| `tdx_stock_analyzer.py` | 个股四维诊断（行情/技术/财务/资金） | `600519 [--json]` |
| `tdx_concept_board.py` | 概念成分股 / 个股概念 / 多概念对比 | `--concept "DeepSeek"` / `--stock 600519` / `--compare A B C` |
| `tdx_limit_up.py` | 今日涨停 / 连板梯队 / 概念集中度 | `--min-boards 2` / `--by-concept` / `--ladder` |
| `tdx_market_daily.py` | 每日市场概览（7 板块并发） | `--section breadth/capital/sectors` |
| `tdx_data_enricher.py` | 批量增补概念/北向/机构/评级/筹码 | `--all` / `--concepts` / `--ratings --codes ...` |

```bash
python -m scripts.tdx_mcp.tdx_stock_analyzer 600519
python -m scripts.tdx_mcp.tdx_concept_board --concept "人形机器人" --all
python -m scripts.tdx_mcp.tdx_limit_up --min-boards 2 --ladder
python -m scripts.tdx_mcp.tdx_market_daily --json
```

### `tdx_data_enricher.py` — 离线数据增补

把 MCP 批量数据写入 `data/`（与 parquet 同目录，JSON 格式），供离线分析：

```bash
python -m scripts.tdx_mcp.tdx_data_enricher --dry-run      # 预览字段清单
python -m scripts.tdx_mcp.tdx_data_enricher --concepts     # 全市场概念标签
python -m scripts.tdx_mcp.tdx_data_enricher --ratings --codes 600519,300750
python -m scripts.tdx_mcp.tdx_data_enricher --all
```

| 输出文件 | 内容 |
|----------|------|
| `data/tdx_concepts.json` | 全市场个股概念标签（最多 47 个/只） |
| `data/tdx_north_money.json` | 今日陆股通活跃股净买量/成交额 |
| `data/tdx_inst_holdings.json` | 机构/基金持仓比例、家数 |
| `data/tdx_analyst_ratings.json` | 分析师评级、目标价、预测 EPS |
| `data/tdx_chip_enhanced.json` | 筹码集中度、获利比例、平均成本 |

> `--ratings` / `--chip` 不传 `--codes` 时，默认从 `data/daily/ts_code=*/` 分区取股票池（本项目无流通市值列，按 ts_code 排序取前 `--max-stocks` 只）。

### Claude Code MCP 工具模式（让 AI 直接查）

项目根创建 `.mcp.json`（**已加入 `.gitignore`**，勿提交 key）：

```json
{
  "mcpServers": {
    "tdx": {
      "type": "http",
      "url": "https://mcp.tdx.com.cn:3001/mcp",
      "headers": { "tdx-api-key": "TDX-your-api-key" }
    }
  }
}
```

`~/.claude/settings.json` 启用 `{"enableAllProjectMcpServers": true}`，重启 Claude Code 后 `claude mcp list` 应见 `tdx: ✔ Connected`。

### 限制

| 限制 | 说明 |
|------|------|
| 单次单品种 | 每次 `tdx_wenda_quotes` 只查 1 只股票或 1 个板块 |
| 资金流盘后为空 | `主力净流入` 等收盘后可能为空 |
| 北向非全量 | 仅当日陆股通活跃前 ~20~50 只 |
| 无 L2 行情 | 不支持逐笔成交、五档盘口 |

---

## 6. DuckDB 全市场查询层

全市场历史行情继续按 `ts_code` 分区保存为 Parquet，DuckDB 直接扫描文件，不需要把
5000 多只股票重复导入另一套数据库，也不会生成全市场巨型 JSON。安装项目依赖：

本 fork 将 `data/` 作为跨设备研究快照纳入 Git。每次行情或证券主表同步完成后，应先在
`tdx-quant` 提交并推送数据，再到父项目提交新的子模块指针；另一台电脑执行
`git submodule update --init --recursive` 后即可取得同一批研究数据。数据提交只解决
跨设备复现，不能代替每日增量同步。

```bash
python -m pip install -r requirements.txt
```

命令行可以检查数据覆盖、搜索股票或查询一只股票的指定区间：

```bash
# 统计每个 Parquet 数据域的行数和证券数
python -m scripts.data_pipeline.query.duckdb_store --data-root data summary

# 从最新证券快照搜索沪深 A 股
python -m scripts.data_pipeline.query.duckdb_store --data-root data \
  symbols --search 茅台 --market SH --limit 20

# 查询单股日线；只返回指定上限内最近的数据，最终按时间升序排列
python -m scripts.data_pipeline.query.duckdb_store --data-root data \
  bars 600519.SH --timeframe daily --start 2025-01-01 --end 2026-07-08 --limit 500
```

Python 服务层可以直接复用同一个接口：

```python
from scripts.data_pipeline.query import DuckDBMarketStore

with DuckDBMarketStore("data") as store:
    stocks = store.list_symbols(search="银行", limit=50)
    bars = store.query_bars("000001", start="2026-01-01", limit=300)
    latest = store.latest_bars(symbols=["000001", "600519"])
```

- `query_bars` 支持 `daily / 5m / 15m / 30m / 60m / index`，按证券与日期裁剪。
- `latest_bars` 使用窗口函数一次取得股票池每只证券的最新 K 线，适合选股预筛。
- `list_symbols` 默认过滤指数、基金、债券和 B 股，只返回沪深 A 股。
- 所有查询均参数化并限制最大返回行数；浏览器后续只需请求当前股票，不读取全市场文件。

---

## 测试

```bash
# 离线测试(纯算法,无需网络)
python -m pytest tests/ -q -m "not integration"

# 全量(含 2 个 pytdx 实盘集成测试,需要能连上通达信服务器)
python -m pytest tests/ -q
```

- `tests/test_indicators_*.py`：每个指标都用合成数据断言具体数值（MA5/RSI/KDJ/BOLL/ATR/量比 等）。
- `tests/test_screener.py` / `test_screener_cli.py`：合成信号、缓存命中、坏票容错、CLI。
- `tests/test_tdx_client_integration.py`：真实下载 `000001` 等，验证 ts_code / parquet 回读 / trade_time。
- `tests/test_pytdx_extended_integration.py`：扩展接口（tick / 分时 / 股本结构 / F10 财务 / 指数 / 枚举）的实盘端到端测试。
