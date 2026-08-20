# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
import json

# file_name = "Permira V - FS - 2024-12-31_Redacted.pdf"
file_name = "260331-FS-Bain X.pdf"
table_id = 2

df = spark.read.table("serverless_stable_14ey07_catalog.hollyport.extracted_table_data_variant_merged")

filtered_df = (
    df.filter((F.col("file_name") == file_name) & (F.col("table_id") == table_id))
    .select("file_name", "table_id", "formatted_table_columns", "data")
)

rows = filtered_df.collect()

# Get column order from formatted_table_columns (same for all rows in a table)
column_order = rows[0]["formatted_table_columns"] if rows else []

# Reorder each record's keys to match formatted_table_columns
records = []
for row in rows:
    if row["data"]:
        record = json.loads(str(row["data"]))
        # Build ordered dict using formatted_table_columns order
        ordered_record = {col: record.get(col, "") for col in column_order if col in record}
        # Append any keys not in formatted_table_columns at the end
        for key in record:
            if key not in ordered_record:
                ordered_record[key] = record[key]
        records.append(ordered_record)

result = {
    "file_name": file_name,
    "table_id": table_id,
    "expected_json": json.dumps(records)
}

# print(f"Column order from formatted_table_columns: {column_order}")
display(result)

# COMMAND ----------

