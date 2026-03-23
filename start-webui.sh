#!/bin/bash
# Start Bedrock Analytics WebUI
# Usage: ./start-webui.sh [--profile PROFILE] [--region REGION]

PROFILE=""
REGION="us-west-2"

while [[ $# -gt 0 ]]; do
    case $1 in
        --profile) PROFILE="$2"; shift 2 ;;
        --region)  REGION="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

export AWS_DEFAULT_REGION="$REGION"
[[ -n "$PROFILE" ]] && export AWS_PROFILE="$PROFILE"

cd "$(dirname "$0")" && uv run python -m webui.index
