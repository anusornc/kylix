#!/usr/bin/env bash
# Load Fig1 through the public API, run the lineage suite, and print the
# paper correctness table (Kylix column filled; Fabric on-chain is not
# answerable).
#
# MIX_ENV=test uses the same test-key path as the Fig1 ExUnit setup.
# Production signing is out of scope for this runner.
set -euo pipefail
cd "$(dirname "$0")/.."
MIX_ENV=test mix eval.kylix
