from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.screener.run_screener import main, screen


class _FakeDownloader:
    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self.frames = frames

    def download_daily(self, code: str) -> pd.DataFrame:
        return self.frames[code].copy()


def _neutral_frame() -> pd.DataFrame:
    close = np.linspace(10.0, 12.0, 120)
    return pd.DataFrame(
        {'open': close, 'high': close + 0.1, 'low': close - 0.1,
         'close': close, 'vol': np.full(120, 1e6), 'amount': close * 1e6}
    )


@pytest.fixture
def _patched_screen(monkeypatch):
    """Replace ``run_screener.screen`` with a fake so the CLI never needs a
    real downloader / parquet cache; capture what the CLI passed it."""
    captured: dict = {}

    def _fake_screen(codes, conditions, *, data_root, downloader=None, max_bars=200):
        captured['codes'] = list(codes)
        captured['condition_names'] = [getattr(c, '__name__', str(c)) for c in conditions]
        captured['data_root'] = data_root
        captured['max_bars'] = max_bars
        return pd.DataFrame(
            [{'ts_code': '000001.SZ', 'timeframe': 'daily', 'close': 12.34,
              'hit_count': 1, 'matched': ['golden_cross'], 'latest_trade_date': '20240101'}]
        )

    import scripts.data_pipeline.screener.run_screener as mod

    monkeypatch.setattr(mod, 'screen', _fake_screen)
    # the CLI calls the module-level ``screen`` name resolved inside main, so
    # also patch the re-export used when invoked via ``screener.main``.
    monkeypatch.setattr('scripts.data_pipeline.screener.screen', _fake_screen, raising=False)
    return captured


def test_cli_inline_codes_prints_table_returns_zero(_patched_screen, capsys) -> None:
    rc = main(['--codes', '000001', '--conditions', 'golden_cross', '--data-root', '/tmp'])
    assert rc == 0
    cap = capsys.readouterr()
    assert '000001.SZ' in cap.out
    assert 'golden_cross' in cap.out
    assert _patched_screen['codes'] == ['000001']
    assert _patched_screen['condition_names'] == ['golden_cross']
    assert _patched_screen['data_root'] == '/tmp'
    assert _patched_screen['max_bars'] == 200  # default cap per timeframe


def test_cli_max_bars_passed_through(_patched_screen) -> None:
    rc = main(['--codes', '000001', '--conditions', 'golden_cross', '--max-bars', '100'])
    assert rc == 0
    assert _patched_screen['max_bars'] == 100


def test_cli_codes_file(_patched_screen, tmp_path) -> None:
    codes_file = tmp_path / 'codes.json'
    codes_file.write_text(json.dumps(['000001', '600000']))
    rc = main([
        '--codes-file', str(codes_file),
        '--conditions', 'golden_cross,rsi_oversold',
    ])
    assert rc == 0
    assert _patched_screen['codes'] == ['000001', '600000']
    assert _patched_screen['condition_names'] == ['golden_cross', 'rsi_oversold']


def test_cli_writes_output_csv(_patched_screen, tmp_path, capsys) -> None:
    out = tmp_path / 'result.csv'
    rc = main([
        '--codes', '000001',
        '--conditions', 'golden_cross',
        '--output', str(out),
    ])
    assert rc == 0
    assert out.exists()
    written = pd.read_csv(out)
    assert list(written.columns)[:1] == ['ts_code']
    # stdout ALSO printed (always)
    assert '000001.SZ' in capsys.readouterr().out


def test_cli_unknown_condition_errors(_patched_screen) -> None:
    with pytest.raises(SystemExit):
        main(['--codes', '000001', '--conditions', 'nope'])


def test_cli_requires_one_code_source(_patched_screen) -> None:
    with pytest.raises(SystemExit):
        main(['--conditions', 'golden_cross'])


def test_cli_codes_and_codes_file_mutually_exclusive(_patched_screen, tmp_path) -> None:
    codes_file = tmp_path / 'codes.json'
    codes_file.write_text('["000001"]')
    with pytest.raises(SystemExit):
        main([
            '--codes', '000002',
            '--codes-file', str(codes_file),
            '--conditions', 'golden_cross',
        ])


def test_cli_runs_as_module(tmp_path, monkeypatch) -> None:
    """End-to-end: invoke the real screen() via the CLI with a fake downloader
    plumbed through ``TdxDownloader`` patching (no network)."""
    import scripts.data_pipeline.screener.run_screener as mod

    class _Stub:
        def __init__(self, data_root) -> None:
            pass

        def download_daily(self, code: str) -> pd.DataFrame:
            return _neutral_frame()

    monkeypatch.setattr(mod, 'TdxDownloader', _Stub)
    rc = main(['--codes', '000001', '--conditions', 'golden_cross', '--data-root', str(tmp_path)])
    assert rc == 0


def test_cli_module_invocable_via_python_m(tmp_path) -> None:
    """``python -m scripts.data_pipeline.screener.run_screener`` loads and runs
    with --help (no screen call), proving the module entry point works."""
    import subprocess
    import sys

    proc = subprocess.run(
        [sys.executable, '-m', 'scripts.data_pipeline.screener.run_screener', '--help'],
        capture_output=True, text=True, cwd=Path(__file__).resolve().parents[1],
        timeout=30,
    )
    assert proc.returncode == 0
    assert '--codes' in proc.stdout
    assert '--conditions' in proc.stdout
    # the package __init__ must not eagerly import run_screener (would double-load)
    assert 'RuntimeWarning' not in proc.stderr
