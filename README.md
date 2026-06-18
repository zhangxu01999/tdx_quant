# tdx_quant — 通达信(pytdx)数据获取 · 指标计算 · 选股

基于 **pytdx** 的 A 股数据管道：下载 → 落盘(parquet) → 计算技术指标 → 条件选股。
仅依赖 pytdx，不接 tushare / baostock；复用 `scripts/data_pipeline/` 已有的多主机轮询连接层。

> 设计与排除项见 [`PLAN.md`](./PLAN.md)。仅支持沪深主板 6 位代码（不做北交所）；单次下载失败即抛异常，批量选股按个股容错。

---

## 目录结构（本次新增）

```
scripts/data_pipeline/
├── tdx_client.py                 # 高层下载封装 TdxDownloader
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
```

---

## 环境依赖

需要 `pytdx`、`pandas`、`pyarrow`、`numpy`（任一满足即可）：

```bash
pip install pytdx pandas pyarrow numpy
```

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

### 落盘格式（按 ts_code 分区，每只股票一个文件）

```
data/<domain>/ts_code=<代码.SZ|SH>/data.parquet
```

`<domain>` ∈ `daily`、`minute_5m|15m|30m|60m`、`xdxr`。每只股票一个文件，含其全部历史（覆盖写，非追加）。

`ts_code` 作为分区键存在路径里（文件内不重复存），读时由 hive 分区还原；`pd.read_parquet('data/daily')` 可一次读回该 domain 下全部股票。

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

