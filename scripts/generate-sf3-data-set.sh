#!/usr/bin/env bash

# Generates the LSQB data set for SF=3 by orchestrating SNB Datagen, the
# data converter and the LSQB preprocessing step.

set -euo pipefail

SF=3
PARALLELISM=${PARALLELISM:-4}
JAVA_MEMORY=${JAVA_MEMORY:-16G}
REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/..

# Allow overriding locations via env vars.
LSQB_DIR=${LSQB_DIR:-${REPO_ROOT}}
CONVERTER_REPOSITORY_DIR=${CONVERTER_REPOSITORY_DIR:-~/git/snb/ldbc_snb_data_converter}

if [[ ! -d "${CONVERTER_REPOSITORY_DIR}" ]]; then
  echo "Missing data converter repo at ${CONVERTER_REPOSITORY_DIR}" >&2
  exit 1
fi

if [[ -z "${DATAGEN_DATA_DIR:-}" ]]; then
  DATAGEN_DIR=${DATAGEN_DIR:-~/git/snb/ldbc_snb_datagen}
  if [[ ! -d "${DATAGEN_DIR}" ]]; then
    echo "Missing LDBC Datagen repo at ${DATAGEN_DIR}" >&2
    exit 1
  fi

  echo "Generating SF=${SF} with LDBC SNB Datagen"
  cd "${DATAGEN_DIR}"
  time ./tools/run.py ./target/ldbc_snb_datagen-0.4.0-SNAPSHOT.jar \
    --parallelism "${PARALLELISM}" \
    --memory "${JAVA_MEMORY}" \
    -- \
    --format csv \
    --mode raw \
    --scale-factor "${SF}" \
    --output-dir "out/sf${SF}-raw"

  export DATAGEN_DATA_DIR="${DATAGEN_DIR}/out/sf${SF}-raw/csv/raw/composite-merged-fk"
else
  echo "Using pre-generated datagen output at ${DATAGEN_DATA_DIR}"
fi

if [[ ! -d "${DATAGEN_DATA_DIR}" ]]; then
  echo "Datagen data directory ${DATAGEN_DATA_DIR} not found" >&2
  exit 1
fi

echo "Running converter pipeline on ${DATAGEN_DATA_DIR}"
cd "${CONVERTER_REPOSITORY_DIR}"
./spark-concat.sh "${DATAGEN_DATA_DIR}"
./load.sh "${DATAGEN_DATA_DIR}" --no-header
./transform.sh

echo "Exporting projected/merged FK CSVs via DuckDB"
cat snb-export-only-ids-projected-fk.sql | ./duckdb ldbc.duckdb
cat snb-export-only-ids-merged-fk.sql    | ./duckdb ldbc.duckdb

PROJECTED_DST="${LSQB_DIR}/data/social-network-sf${SF}-projected-fk"
MERGED_DST="${LSQB_DIR}/data/social-network-sf${SF}-merged-fk"

echo "Copying converted CSVs into LSQB repo"
rm -rf "${PROJECTED_DST}" "${MERGED_DST}"
cp -r data/csv-only-ids-projected-fk "${PROJECTED_DST}"
cp -r data/csv-only-ids-merged-fk    "${MERGED_DST}"

echo "Normalizing headers for SF=${SF}"
cd "${LSQB_DIR}"
export SF
scripts/preprocess.sh

echo "Finished generating SF=${SF} data under ${PROJECTED_DST} and ${MERGED_DST}"
