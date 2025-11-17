#!/usr/bin/env bash

# Generates the SF=3 data set (via scripts/generate-sf3-data-set.sh) and then
# imports it into Kùzu using the existing init-and-load pipeline.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
REPO_ROOT="${SCRIPT_DIR}/.."

echo "Generating SF=3 data set"
"${REPO_ROOT}/scripts/generate-sf3-data-set.sh"

echo "Importing SF=3 data set into Kùzu"
cd "${REPO_ROOT}"
export SF=3
kuzu/init-and-load.sh

echo "SF=3 data loaded into Kùzu (database under kuzu/scratch)"
