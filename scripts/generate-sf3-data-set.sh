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

normalize_hadoop_layout_if_needed() {
  local raw_dir="$1"
  if compgen -G "${raw_dir}/static/organisation_"*".csv" >/dev/null; then
    echo "Detected Hadoop Datagen CSV layout; normalizing filenames for converter"
    local normalized_dir
    normalized_dir=$(mktemp -d -t lsqb-datagen-XXXXXX)
    mkdir -p "${normalized_dir}/static" "${normalized_dir}/dynamic"

    declare -A STATIC_MAP=(
      ["organisation"]="Organisation"
      ["place"]="Place"
      ["tag"]="Tag"
      ["tagclass"]="TagClass"
    )
    declare -A DYNAMIC_MAP=(
      ["comment"]="Comment"
      ["comment_hasTag_tag"]="Comment_hasTag_Tag"
      ["forum"]="Forum"
      ["forum_hasMember_person"]="Forum_hasMember_Person"
      ["forum_hasTag_tag"]="Forum_hasTag_Tag"
      ["person"]="Person"
      ["person_hasInterest_tag"]="Person_hasInterest_Tag"
      ["person_knows_person"]="Person_knows_Person"
      ["person_likes_comment"]="Person_likes_Comment"
      ["person_likes_post"]="Person_likes_Post"
      ["person_studyAt_organisation"]="Person_studyAt_University"
      ["person_workAt_organisation"]="Person_workAt_Company"
      ["post"]="Post"
      ["post_hasTag_tag"]="Post_hasTag_Tag"
    )

    for file in "${raw_dir}/static"/*.csv; do
      local base name target
      base=$(basename "${file}")
      name=$(echo "${base}" | sed -E 's/_[0-9]+_[0-9]+\.csv$//')
      target=${STATIC_MAP[${name}]}
      if [[ -z "${target:-}" ]]; then
        echo "Unknown static file ${base}, cannot normalize" >&2
        exit 1
      fi
      tail -n +2 "${file}" > "${normalized_dir}/static/${target}.csv"
    done

    for file in "${raw_dir}/dynamic"/*.csv; do
      local base name target
      base=$(basename "${file}")
      name=$(echo "${base}" | sed -E 's/_[0-9]+_[0-9]+\.csv$//')
      target=${DYNAMIC_MAP[${name}]}
      if [[ -z "${target:-}" ]]; then
        echo "Unknown dynamic file ${base}, cannot normalize" >&2
        exit 1
      fi
      tail -n +2 "${file}" > "${normalized_dir}/dynamic/${target}.csv"
    done

    DATAGEN_DATA_DIR="${normalized_dir}"
  fi
}

normalize_hadoop_layout_if_needed "${DATAGEN_DATA_DIR}"

if [[ -d "${DATAGEN_DATA_DIR}/static/Organisation" ]]; then
  ORGANISATION_SAMPLE=$(find "${DATAGEN_DATA_DIR}/static/Organisation" -maxdepth 1 -type f -name '*.csv' | head -n 1 || true)
  if [[ -n "${ORGANISATION_SAMPLE}" ]]; then
    ORGANISATION_COLUMN_COUNT=$(head -n 1 "${ORGANISATION_SAMPLE}" | tr -d '\r' | awk -F'|' '{print NF}')
    if [[ "${ORGANISATION_COLUMN_COUNT}" -lt 5 ]]; then
      cat >&2 <<EOF
The directory at ${DATAGEN_DATA_DIR} appears to contain CsvCompositeProjectedFK files
(detected ${ORGANISATION_COLUMN_COUNT} columns in static/Organisation/*.csv).
The LSQB conversion pipeline requires raw CsvCompositeMergedFK output from the Java datagen
under csv/raw/composite-merged-fk. Please point DATAGEN_DATA_DIR to that directory.
EOF
      exit 1
    fi
  fi
fi

if [[ -d "${DATAGEN_DATA_DIR}/dynamic/Comment" ]]; then
  COMMENT_SAMPLE=$(find "${DATAGEN_DATA_DIR}/dynamic/Comment" -maxdepth 1 -type f -name '*.csv' | head -n 1 || true)
  if [[ -n "${COMMENT_SAMPLE}" ]]; then
    COMMENT_COLUMN_COUNT=$(head -n 1 "${COMMENT_SAMPLE}" | tr -d '\r' | awk -F'|' '{print NF}')
    if [[ "${COMMENT_COLUMN_COUNT}" -lt 12 ]]; then
      cat >&2 <<EOF
The directory at ${DATAGEN_DATA_DIR} appears to be a BI CsvCompositeMergedFK snapshot (detected ${COMMENT_COLUMN_COUNT} columns in dynamic/Comment/*.csv).
The LSQB conversion pipeline needs the raw CsvCompositeMergedFK output (java datagen --mode raw, under csv/raw/composite-merged-fk).
Please re-run datagen in raw mode or download the raw data set and point DATAGEN_DATA_DIR there.
EOF
      exit 1
    fi
  fi
fi

echo "Running converter pipeline on ${DATAGEN_DATA_DIR}"
cd "${CONVERTER_REPOSITORY_DIR}"
if compgen -G "${DATAGEN_DATA_DIR}/static"/*/*.csv >/dev/null; then
  ./spark-concat.sh "${DATAGEN_DATA_DIR}"
else
  echo "Skipping spark-concat.sh – looks like CSV files are already concatenated"
fi
./load.sh "${DATAGEN_DATA_DIR}" --no-header
./transform.sh

echo "Exporting projected/merged FK CSVs via DuckDB"
cat export/snb-export-only-ids-projected-fk.sql | ./duckdb ldbc.duckdb
cat export/snb-export-only-ids-merged-fk.sql    | ./duckdb ldbc.duckdb

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
