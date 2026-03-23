"""Bedrock Invocation Analytics WebUI — Entry point."""

import tomllib
from pathlib import Path

from webui import dashboard  # noqa: F401
from webui import pricing  # noqa: F401
from nicegui import ui

with open(Path(__file__).parent.parent / "pyproject.toml", "rb") as f:
    VERSION = tomllib.load(f)["project"]["version"]

# Expose version for other modules
dashboard.VERSION = VERSION

ui.run(title="Bedrock Invocation Analytics", favicon="docs/favicon.svg", port=8080, reload=False)
