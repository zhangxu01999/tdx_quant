/* app.js — A股量化数据终端
 * Loads exported JSON (frontend/assets/*.json) and renders ECharts panels.
 * Chinese market convention: 涨=红(#ff4d6d) / 跌=绿(#00d68f).
 */
'use strict';

const C = {
  up: '#ff4d6d', down: '#00d68f', amber: '#f0b90b',
  blue: '#4ea1ff', purple: '#b794ff', orange: '#ff9f43',
  muted: '#8a93a6', grid: '#20283a', text: '#c7cdd9',
};
const AXIS = { axisLine: { lineStyle: { color: C.grid } }, axisLabel: { color: C.muted }, splitLine: { lineStyle: { color: '#161c2a' } }, axisTick: { lineStyle: { color: C.grid } } };
const charts = {};
const rendered = new Set();
let DATA = {};

/* ---------- helpers ---------- */
function $(id) { return document.getElementById(id); }
function el(cls, html) { const d = document.createElement('div'); d.className = cls; d.innerHTML = html; return d; }
function fmtBig(n) {
  if (n == null || isNaN(n)) return '—';
  const a = Math.abs(n);
  if (a >= 1e8) return (n / 1e8).toFixed(2) + ' 亿';
  if (a >= 1e4) return (n / 1e4).toFixed(2) + ' 万';
  return (+n).toLocaleString();
}
function fmtNum(n, d = 2) { return (n == null || isNaN(n)) ? '—' : (+n).toFixed(d); }
function pct(now, prev) { return prev ? ((now - prev) / prev) * 100 : null; }
function minuteLabel(i) {
  // i: 0..239, 0=09:30, lunch break 11:30..13:00
  let h, m;
  if (i < 120) { h = 9; m = 30 + i; }
  else { h = 13; m = i - 120; }
  const hh = h + Math.floor(m / 60), mm = m % 60;
  return String(hh).padStart(2, '0') + ':' + String(mm).padStart(2, '0');
}
function chart(id) {
  if (!charts[id]) charts[id] = echarts.init($(id), null, { renderer: 'canvas' });
  return charts[id];
}

/* ---------- load ---------- */
async function load() {
  const files = ['overview', 'kline_daily', 'minute', 'ticks', 'fundamentals'];
  const out = await Promise.all(files.map(f =>
    fetch('assets/' + f + '.json', { cache: 'no-store' }).then(r => {
      if (!r.ok) throw new Error(r.status + ' ' + f);
      return r.json();
    })
  ));
  files.forEach((f, i) => DATA[f.replace('_daily', '')] = out[i]);
  DATA.kline = out[1];
}

/* ---------- overview ---------- */
function renderOverview() {
  const d = DATA.overview;
  const sh = d.indices.find(x => x.ts_code.endsWith('.SH')) || d.indices[0];
  const sz = d.indices.find(x => x.ts_code.endsWith('.SZ')) || d.indices[1];
  const asof = sh.points.length ? sh.points[sh.points.length - 1].trade_date : '—';

  $('topbar-stats').innerHTML = [
    statCard('数据基准日', asof, 'amber'),
    statCard('证券总数', fmtBig(d.universe.total || 0), ''),
    statCard('沪市 SH', fmtBig(d.universe.SH || 0), ''),
    statCard('深市 SZ', fmtBig(d.universe.SZ || 0), ''),
  ].join('');

  function idxCard(idx) {
    const p = idx.points;
    if (!p.length) return '';
    const last = p[p.length - 1], prev = p[p.length - 2] || last;
    const chg = pct(last.close, prev.close);
    const cls = chg >= 0 ? 'up' : 'down';
    const arrow = chg >= 0 ? '▲' : '▼';
    return `<div class="card"><div class="label">${idx.name} · ${idx.ts_code}</div>
      <div class="value ${cls}">${fmtNum(last.close)}</div>
      <div class="sub"><span class="delta ${cls}">${arrow} ${fmtNum(Math.abs(chg), 2)}%</span>
        <span class="muted"> 涨 ${last.up_count} / 跌 ${last.down_count}</span></div></div>`;
  }
  $('index-cards').innerHTML = idxCard(sh) + idxCard(sz);

  // breadth diverging bar (上证)
  const dates = sh.points.map(p => p.trade_date.slice(4));
  chart('ch-breadth').setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: ['上涨家数', '下跌家数'], textStyle: { color: C.text }, top: 0 },
    grid: { left: 44, right: 16, top: 36, bottom: 28 },
    xAxis: { type: 'category', data: dates, ...AXIS },
    yAxis: { type: 'value', ...AXIS },
    series: [
      { name: '上涨家数', type: 'bar', stack: 'b', color: C.up, data: sh.points.map(p => p.up_count) },
      { name: '下跌家数', type: 'bar', stack: 'b', color: C.down, data: sh.points.map(p => -p.down_count) },
    ],
  });

  // index dual-line
  const xsz = sz.points.map(p => p.trade_date.slice(4));
  chart('ch-index-line').setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: [sh.name, sz.name], textStyle: { color: C.text }, top: 0 },
    grid: { left: 50, right: 60, top: 36, bottom: 28 },
    xAxis: { type: 'category', data: sh.points.map(p => p.trade_date.slice(4)), ...AXIS },
    yAxis: [
      { type: 'value', scale: true, name: sh.name, nameTextStyle: { color: C.muted }, ...AXIS },
      { type: 'value', scale: true, name: sz.name, nameTextStyle: { color: C.muted }, ...AXIS },
    ],
    series: [
      { name: sh.name, type: 'line', smooth: true, symbol: 'none', color: C.up, data: sh.points.map(p => p.close) },
      { name: sz.name, type: 'line', smooth: true, symbol: 'none', yAxisIndex: 1, color: C.blue, data: sz.points.map(p => p.close) },
    ],
  });
}

/* ---------- kline ---------- */
function renderKline() {
  const d = DATA.kline;
  const L = d.latest || {};
  const histCls = L.macd_hist >= 0 ? 'up' : 'down';
  const rsiCls = L.rsi6 == null ? '' : (L.rsi6 < 30 ? 'down' : L.rsi6 > 70 ? 'up' : '');
  $('kline-title').textContent = `${d.name} (${d.ts_code}) · 日线 ${d.bars} 根`;
  $('kline-cards').innerHTML = [
    `<div class="card"><div class="label">最新收盘</div><div class="value">${fmtNum(L.close)}</div><div class="sub muted">末交易日</div></div>`,
    `<div class="card"><div class="label">MA5 / MA10 / MA20</div><div class="value" style="font-size:20px">${fmtNum(L.ma5)} · ${fmtNum(L.ma10)} · ${fmtNum(L.ma20)}</div><div class="sub muted">均线粘合度</div></div>`,
    `<div class="card"><div class="label">RSI6</div><div class="value ${rsiCls}">${fmtNum(L.rsi6)}</div><div class="sub muted">${L.rsi6 != null && L.rsi6 < 30 ? '超卖区' : L.rsi6 > 70 ? '超买区' : '中性区'}</div></div>`,
    `<div class="card"><div class="label">MACD 柱</div><div class="value ${histCls}">${fmtNum(L.macd_hist, 4)}</div><div class="sub muted">${histCls === 'up' ? '多头' : '空头'}动能</div></div>`,
  ].join('');

  const n = d.dates.length;
  const volColors = d.ohlc.map(o => o[1] >= o[0] ? C.up : C.down);

  // main: price(candle+MA+BOLL) + volume, two grids
  chart('ch-kline').setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['K线', 'MA5', 'MA20', 'BOLL上', 'BOLL下'], textStyle: { color: C.text }, top: 0 },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    grid: [
      { left: 56, right: 16, top: 36, height: '58%' },
      { left: 56, right: 16, top: '76%', height: '16%' },
    ],
    xAxis: [
      { type: 'category', data: d.dates, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: Math.floor(n / 10) }, gridIndex: 0 },
      { type: 'category', data: d.dates, ...AXIS, gridIndex: 1, axisLabel: { show: false } },
    ],
    yAxis: [
      { scale: true, gridIndex: 0, ...AXIS },
      { scale: true, gridIndex: 1, splitNumber: 2, ...AXIS },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 60, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start: 60, end: 100, height: 18, bottom: 4, borderColor: C.grid, fillerColor: 'rgba(240,185,11,.08)' },
    ],
    series: [
      { name: 'K线', type: 'candlestick', data: d.ohlc, xAxisIndex: 0, yAxisIndex: 0,
        itemStyle: { color: C.up, color0: C.down, borderColor: C.up, borderColor0: C.down } },
      { name: 'MA5', type: 'line', data: d.ma.MA5, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', smooth: true, lineStyle: { color: C.amber, width: 1.5 } },
      { name: 'MA20', type: 'line', data: d.ma.MA20, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', smooth: true, lineStyle: { color: C.purple, width: 1.5 } },
      { name: 'BOLL上', type: 'line', data: d.boll.UP, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', lineStyle: { color: C.blue, width: 1, type: 'dashed', opacity: .7 } },
      { name: 'BOLL下', type: 'line', data: d.boll.DN, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', lineStyle: { color: C.blue, width: 1, type: 'dashed', opacity: .7 } },
      { name: '成交量', type: 'bar', data: d.vol.map((v, i) => ({ value: v, itemStyle: { color: volColors[i], opacity: .55 } })), xAxisIndex: 1, yAxisIndex: 1 },
    ],
  });

  // MACD
  chart('ch-macd').setOption({
    tooltip: { trigger: 'axis' }, legend: { data: ['DIF', 'DEA', 'MACD'], textStyle: { color: C.text }, top: 0 },
    grid: { left: 50, right: 16, top: 36, bottom: 30 },
    xAxis: { type: 'category', data: d.dates, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: Math.floor(n / 8) } },
    yAxis: { type: 'value', ...AXIS },
    dataZoom: [{ type: 'inside', start: 60, end: 100 }],
    series: [
      { name: 'MACD', type: 'bar', data: d.macd.HIST.map(v => ({ value: v, itemStyle: { color: v >= 0 ? C.up : C.down } })) },
      { name: 'DIF', type: 'line', data: d.macd.DIF, symbol: 'none', smooth: true, lineStyle: { color: C.amber } },
      { name: 'DEA', type: 'line', data: d.macd.DEA, symbol: 'none', smooth: true, lineStyle: { color: C.blue } },
    ],
  });

  // RSI
  chart('ch-rsi').setOption({
    tooltip: { trigger: 'axis' }, legend: { data: ['RSI6', 'RSI12'], textStyle: { color: C.text }, top: 0 },
    grid: { left: 50, right: 16, top: 36, bottom: 30 },
    xAxis: { type: 'category', data: d.dates, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: Math.floor(n / 8) } },
    yAxis: { type: 'value', min: 0, max: 100, ...AXIS },
    dataZoom: [{ type: 'inside', start: 60, end: 100 }],
    series: [
      { name: 'RSI6', type: 'line', data: d.rsi.RSI6, symbol: 'none', smooth: true, lineStyle: { color: C.up } },
      { name: 'RSI12', type: 'line', data: d.rsi.RSI12, symbol: 'none', smooth: true, lineStyle: { color: C.blue } },
    ],
  });
  // markLines for 30/70
  chart('ch-rsi').setOption({ series: [
    { markLine: { silent: true, symbol: 'none', lineStyle: { color: C.muted, type: 'dashed' }, data: [{ yAxis: 30 }, { yAxis: 70 }] } },
  ] });

  // KDJ
  chart('ch-kdj').setOption({
    tooltip: { trigger: 'axis' }, legend: { data: ['K', 'D', 'J'], textStyle: { color: C.text }, top: 0 },
    grid: { left: 50, right: 16, top: 30, bottom: 30 },
    xAxis: { type: 'category', data: d.dates, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: Math.floor(n / 8) } },
    yAxis: { type: 'value', scale: true, ...AXIS },
    dataZoom: [{ type: 'inside', start: 60, end: 100 }],
    series: [
      { name: 'K', type: 'line', data: d.kdj.K, symbol: 'none', smooth: true, lineStyle: { color: C.amber } },
      { name: 'D', type: 'line', data: d.kdj.D, symbol: 'none', smooth: true, lineStyle: { color: C.blue } },
      { name: 'J', type: 'line', data: d.kdj.J, symbol: 'none', smooth: true, lineStyle: { color: C.purple } },
    ],
  });

  // connect for synced zoom/tooltip
  ['ch-kline', 'ch-macd', 'ch-rsi', 'ch-kdj'].forEach(id => { chart(id).group = 'kline'; });
  echarts.connect('kline');
}

/* ---------- minute ---------- */
function renderMinute() {
  const d = DATA.minute;
  if (!$('min-sym').dataset.bound) {
    $('min-sym').dataset.bound = '1';
    d.symbols.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = `${d.names[s]} (${s})`; $('min-sym').appendChild(o); });
    d.timeframes.forEach(t => { const o = document.createElement('option'); o.value = t; o.textContent = t + '分钟'; $('min-tf').appendChild(o); });
    $('min-sym').addEventListener('change', drawMinute);
    $('min-tf').addEventListener('change', drawMinute);
  }
  drawMinute();
}
function drawMinute() {
  const d = DATA.minute;
  const sym = $('min-sym').value || d.symbols[0];
  const tf = $('min-tf').value || d.timeframes[0];
  const seg = d.data[sym] && d.data[sym][tf];
  if (!seg) return;
  const n = seg.dates.length;
  const volColors = seg.ohlc.map(o => o[1] >= o[0] ? C.up : C.down);
  $('min-title').textContent = `${d.names[sym]} (${sym}) · ${tf} 分钟线 · ${n} 根`;
  $('min-meta').textContent = `${n} 根 K 线 · ${seg.dates[0]} → ${seg.dates[n - 1]}`;
  chart('ch-minute').setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['K线', '成交量'], textStyle: { color: C.text }, top: 0 },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    grid: [{ left: 50, right: 16, top: 36, height: '64%' }, { left: 50, right: 16, top: '78%', height: '14%' }],
    xAxis: [
      { type: 'category', data: seg.dates, ...AXIS, gridIndex: 0, axisLabel: { ...AXIS.axisLabel, interval: Math.floor(n / 8) } },
      { type: 'category', data: seg.dates, ...AXIS, gridIndex: 1, axisLabel: { show: false } },
    ],
    yAxis: [{ scale: true, gridIndex: 0, ...AXIS }, { scale: true, gridIndex: 1, splitNumber: 2, ...AXIS }],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 50, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start: 50, end: 100, height: 16, bottom: 4, borderColor: C.grid, fillerColor: 'rgba(240,185,11,.08)' },
    ],
    series: [
      { name: 'K线', type: 'candlestick', data: seg.ohlc, xAxisIndex: 0, yAxisIndex: 0,
        itemStyle: { color: C.up, color0: C.down, borderColor: C.up, borderColor0: C.down } },
      { name: '成交量', type: 'bar', data: seg.vol.map((v, i) => ({ value: v, itemStyle: { color: volColors[i], opacity: .55 } })), xAxisIndex: 1, yAxisIndex: 1 },
    ],
  }, true);
}

/* ---------- ticks ---------- */
function renderTicks() {
  const d = DATA.ticks;
  $('tick-cards').innerHTML = [
    `<div class="card"><div class="label">${d.name} (${d.ts_code})</div><div class="value" style="font-size:20px">${d.date}</div><div class="sub muted">逐笔交易日</div></div>`,
    `<div class="card"><div class="label">成交笔数</div><div class="value">${d.n_ticks.toLocaleString()}</div><div class="sub muted">当日逐笔</div></div>`,
    `<div class="card"><div class="label">价格区间</div><div class="value" style="font-size:20px">${fmtNum(d.price_range[0])} ~ ${fmtNum(d.price_range[1])}</div><div class="sub muted">日内振幅 ${fmtNum(((d.price_range[1] - d.price_range[0]) / d.price_range[0]) * 100, 2)}%</div></div>`,
    `<div class="card"><div class="label">主买 / 主卖</div><div class="value" style="font-size:20px"><span class="up">${d.distribution.buy.toLocaleString()}</span> / <span class="down">${d.distribution.sell.toLocaleString()}</span></div><div class="sub muted">买卖盘笔数</div></div>`,
  ].join('');

  // distribution donut
  chart('ch-tick-dist').setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: { bottom: 0, textStyle: { color: C.text } },
    series: [{
      type: 'pie', radius: ['42%', '68%'], center: ['50%', '46%'],
      itemStyle: { borderColor: '#11151f', borderWidth: 2 },
      label: { color: C.text },
      data: [
        { name: '主买', value: d.distribution.buy, itemStyle: { color: C.up } },
        { name: '主卖', value: d.distribution.sell, itemStyle: { color: C.down } },
        { name: '其他', value: (d.distribution.other || 0) + (d.distribution.neutral || 0), itemStyle: { color: C.muted } },
      ],
    }],
  });

  // price curve
  const xs = d.price_curve.map(p => minuteLabel(p.minute));
  chart('ch-tick-price').setOption({
    tooltip: { trigger: 'axis' }, legend: { data: ['分时价'], textStyle: { color: C.text }, top: 0 },
    grid: { left: 50, right: 16, top: 30, bottom: 30 },
    xAxis: { type: 'category', data: xs, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: 29 } },
    yAxis: { type: 'value', scale: true, ...AXIS },
    series: [{
      name: '分时价', type: 'line', data: d.price_curve.map(p => p.price), symbol: 'none',
      smooth: true, lineStyle: { color: C.amber }, areaStyle: { color: 'rgba(240,185,11,.12)' },
      markLine: { silent: true, symbol: 'none', lineStyle: { color: C.muted, type: 'dotted' }, data: [{ type: 'average', name: '均价' }] },
    }],
  });

  // order-flow diverging bar
  chart('ch-tick-flow').setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: ['主买量', '主卖量'], textStyle: { color: C.text }, top: 0 },
    grid: { left: 50, right: 16, top: 30, bottom: 30 },
    xAxis: { type: 'category', data: d.flow.map(f => minuteLabel(f.minute)), ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: 29 } },
    yAxis: { type: 'value', ...AXIS },
    dataZoom: [{ type: 'inside', start: 0, end: 100 }],
    series: [
      { name: '主买量', type: 'bar', color: C.up, data: d.flow.map(f => f.buy_vol) },
      { name: '主卖量', type: 'bar', color: C.down, data: d.flow.map(f => -f.sell_vol) },
    ],
  });
}

/* ---------- fundamentals ---------- */
function renderFundamentals() {
  const d = DATA.fundamentals;
  const np = d.metrics['净利润(元)'] || {};
  const eps = d.metrics['基本每股收益(元)'] || {};
  const lastPeriod = d.periods[d.periods.length - 1];
  const cap = d.capital || {};
  $('fund-cards').innerHTML = [
    `<div class="card"><div class="label">${d.name} (${d.ts_code})</div><div class="value" style="font-size:20px">基本面</div><div class="sub muted">F10 · ${d.periods.length} 个报告期</div></div>`,
    `<div class="card"><div class="label">总股本</div><div class="value" style="font-size:20px">${fmtBig(cap.zongguben)}</div><div class="sub muted">流通 ${fmtBig(cap.liutongguben)}</div></div>`,
    `<div class="card"><div class="label">上市日期</div><div class="value" style="font-size:20px">${fmtIpo(cap.ipo_date)}</div><div class="sub muted">行业码 ${cap.industry_code || '—'} · 地区 ${cap.province_code || '—'}</div></div>`,
    `<div class="card"><div class="label">最新净利润 (${lastPeriod})</div><div class="value" style="font-size:20px">${fmtBig(np[lastPeriod])}</div><div class="sub muted">EPS ${fmtNum(eps[lastPeriod])} 元</div></div>`,
  ].join('');

  if (!$('fund-metric').dataset.bound) {
    $('fund-metric').dataset.bound = '1';
    Object.keys(d.metrics).forEach(m => { const o = document.createElement('option'); o.value = m; o.textContent = m; $('fund-metric').appendChild(o); });
    // prefer a meaningful default
    const pref = ['基本每股收益(元)', '净利润(元)', '加权净资产收益率(%)', '营业总收入(元)'].find(m => d.metrics[m]);
    if (pref) $('fund-metric').value = pref;
    $('fund-metric').addEventListener('change', drawFund);
  }
  drawFund();

  $('fund-info').textContent = d.company_info && d.company_info.trim() ? d.company_info : '（暂无 F10 公司资料文本）';
}
function fmtIpo(s) {
  s = String(s || '');
  return s.length === 8 ? s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6) : (s || '—');
}
function drawFund() {
  const d = DATA.fundamentals;
  const metric = $('fund-metric').value;
  const row = d.metrics[metric] || {};
  const isPct = metric.includes('%') || metric.includes('率');
  const xs = d.periods.map(p => p.slice(0, 7));
  const ys = d.periods.map(p => row[p]);
  const big = metric.includes('(元)');
  const fmt = v => big ? fmtBig(v) : fmtNum(v, 2);
  $('fund-title').textContent = `${metric} · ${d.periods[0].slice(0, 4)}–${d.periods[d.periods.length - 1].slice(0, 4)}`;
  chart('ch-fund').setOption({
    tooltip: { trigger: 'axis', formatter: p => `${p[0].axisValue}<br/>${metric}: <b>${fmt(p[0].data)}</b>` },
    grid: { left: 64, right: 24, top: 36, bottom: 40 },
    xAxis: { type: 'category', data: xs, ...AXIS },
    yAxis: { type: 'value', axisLabel: { color: C.muted, formatter: v => big ? (v / 1e8).toFixed(0) + '亿' : v }, ...AXIS },
    series: [{
      type: isPct ? 'line' : 'bar', data: ys, barWidth: '46%',
      itemStyle: { color: isPct ? C.amber : new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: C.up }, { offset: 1, color: 'rgba(255,77,109,.25)' }]), borderRadius: [4, 4, 0, 0] },
      lineStyle: { color: C.amber, width: 2 }, symbol: 'circle', symbolSize: 7,
      label: { show: true, position: 'top', color: C.text, formatter: p => fmt(p.value) },
    }],
  }, true);
}

/* ---------- stat card helper ---------- */
function statCard(k, v, cls) {
  return `<div class="stat"><div class="k">${k}</div><div class="v ${cls}">${v}</div></div>`;
}

/* ---------- tab wiring ---------- */
const RENDERS = { overview: renderOverview, kline: renderKline, minute: renderMinute, ticks: renderTicks, fundamentals: renderFundamentals };
function activate(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
  document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.dataset.panel === tab));
  // resize existing then render
  requestAnimationFrame(() => {
    const fn = RENDERS[tab];
    if (fn) fn();
    Object.keys(charts).forEach(id => { if ($(id) && charts[id]) charts[id].resize(); });
    rendered.add(tab);
  });
}

window.addEventListener('resize', () => Object.values(charts).forEach(c => c && c.resize()));

(async function init() {
  $('footer-meta').textContent = '加载中…';
  try {
    await load();
  } catch (e) {
    document.querySelector('.content').innerHTML = `<div class="chart-box"><h3>数据加载失败</h3><pre class="f10">${e}\n\n请先在 frontend/ 下运行: python3 data_export.py</pre></div>`;
    $('footer-meta').textContent = 'error';
    return;
  }
  document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => { location.hash = t.dataset.tab; }));
  window.addEventListener('hashchange', () => { const t = location.hash.replace('#', ''); if (RENDERS[t]) activate(t); });
  const fromHash = location.hash.replace('#', '');
  activate(RENDERS[fromHash] ? fromHash : 'overview');
  $('footer-meta').textContent = '渲染完成 · 5 个数据域';
})();
