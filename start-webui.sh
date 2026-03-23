#!/usr/bin/env bash
cd "$(dirname "$0")/webui"
uv sync --quiet
uv run streamlit run app.py "$@"
