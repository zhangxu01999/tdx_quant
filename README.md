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

把 `data/` 下的 parquet 导出为 JSON，用纯静态页面 + ECharts 离线渲染（无需后端服务）。

```bash
cd frontend
python3 data_export.py          # 读 data/*.parquet -> assets/*.json
python3 -m http.server 8765     # 任选端口本地预览
# 浏览器打开 http://127.0.0.1:8765/
```

- **数据导出**：`data_export.py` 读取 `data/` 下全部域，写出 5 个 JSON（`overview / kline_daily / minute / ticks / fundamentals`）。重新下载数据后重跑即可刷新。
- **页面**：`index.html` + `app.js` + `styles.css`；ECharts 已 vendor 在 `assets/echarts.min.js`，**完全离线、无 CDN 依赖**。

### 视图 ↔ 数据来源

每个视图消费的数据域（对应上面的下载接口）：

| 视图 | JSON | 消费数据域（下载接口） |
|------|------|------------------------|
| 1. 市场概览 | `overview.json` | `index_daily`（`download_index`）+ `security_list`（`download_security_list`） |
| 2. K 线主图 | `kline_daily.json` | `data/000001.SZ_indicators.parquet`（见下方说明） |
| 3. 多周期分时 | `minute.json` | `minute_5m/15m/30m/60m`（`download_minute`） |
| 4. 逐笔成交 | `ticks.json` | `tdx_transactions`（`download_tick`）+ `minute_time`（`download_minute_time`） |
| 5. 公司基本面 | `fundamentals.json` | `company_finance`（`download_company_finance`）+ `finance_capital`（`download_finance_capital`）+ `company_info_raw` |

> K 线主图需要一份**预计算指标**文件 `data/000001.SZ_indicators.parquet`（日 K 叠加全部指标列）。它由 `compute_all` 生成，不在下载流程里，需手动产出一次：
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
