from __future__ import annotations

# NOTE: do NOT import from ``run_screener`` here. That submodule is also the
# ``python -m scripts.data_pipeline.screener.run_screener`` entry point; importing
# it eagerly from the package init makes runpy load it twice and emit a
# RuntimeWarning on every CLI invocation. Import ``screen``/``main`` directly:
#     from scripts.data_pipeline.screener.run_screener import screen, main
from scripts.data_pipeline.screener.conditions import CONDITIONS

__all__ = ['CONDITIONS']
