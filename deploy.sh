#!/bin/bash
# Deploy CDK stack
# Usage: ./deploy.sh [cdk-command] [options]
# Examples:
#   ./deploy.sh synth
#   ./deploy.sh deploy --profile lab
#   ./deploy.sh diff

cd "$(dirname "$0")/deploy" && uv run --project .. cdk "$@"
