# How to use

```bash
# 必须使用 Spark Datagen (--mode raw) 。
# Spark BI 快照（graphs/csv/bi/...）不符合转换器要求。
export DATAGEN_DATA_DIR=/home/gary/ldbc_snb/ldbc_snb_datagen_spark-0.5.1/out_sf3_raw/graphs/csv/raw/composite-merged-fk
export CONVERTER_REPOSITORY_DIR=/home/gary/ldbc_snb/ldbc_snb_data_converter
# export BULK_LOAD_DATE="2025-11-18"

./generate-sf3-and-load-kuzu.sh
```