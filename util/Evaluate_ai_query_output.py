# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup
# MAGIC %pip install --upgrade mlflow[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Create ground truth table (run once to bootstrap schema)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# from mlflow.genai.scorers import (
#     Correctness,
#     RetrievalSufficiency,
#     RetrievalGroundedness,
#     RelevanceToQuery,
#     Guidelines,
#     scorer,
# )

# ---------------------------------------------------------------------------
# Ground truth table schema.
# Populate this table with manually verified JSON outputs for a representative
# sample of table elements. Each row = one HTML table's expected extraction.
# ---------------------------------------------------------------------------
GROUND_TRUTH_TABLE = "serverless_stable_14ey07_catalog.hollyport.extraction_ground_truth"

gt_schema = StructType([
    StructField("file_name", StringType(), False),
    StructField("table_id", IntegerType(), False),
    StructField("expected_json", StringType(), False),  # JSON array string
])

# Create table if it doesn't exist
if not spark.catalog.tableExists(GROUND_TRUTH_TABLE):
    spark.createDataFrame([], gt_schema).write.saveAsTable(GROUND_TRUTH_TABLE)
    print(f"Created empty ground truth table: {GROUND_TRUTH_TABLE}")
    print("Populate it with verified expected_json values before running evaluation.")
else:
    gt_count = spark.table(GROUND_TRUTH_TABLE).count()
    print(f"Ground truth table exists with {gt_count} records.")

# COMMAND ----------

# DBTITLE 1,Example: Insert sample ground truth records
import json

sample_ground_truth = [
{'file_name': '260331-FS-Bain X.pdf',
 'table_id': 1,
 'expected_json': '[{"Financial Statements:": "Statement of Assets, Liabilities and Partners\' Capital as of March 31, 2026 (unaudited)", "Page": "1"}, {"Financial Statements:": "Statement of Operations for the three months ended March 31, 2026 (unaudited)", "Page": "2"}, {"Financial Statements:": "Statement of Changes in Partners\' Capital for the three months ended March 31, 2026 (unaudited)", "Page": "3"}, {"Financial Statements:": "Schedule of Investments as of March 31, 2026 (unaudited)", "Page": "4"}]'},

 {'file_name': '260331-FS-Bain X.pdf',
 'table_id': 2,
 'expected_json': '[{"column1_llm": "Cash and cash equivalents", "column2_llm": "$ 6,382,983"}, {"column1_llm": "Investments at fair value (cost of $876,246,649)", "column2_llm": "763,645,401"}, {"column1_llm": "Other assets", "column2_llm": "655,254"}, {"column1_llm": "Total assets", "column2_llm": "770,683,63800"}]'}



 
]



if sample_ground_truth:
    gt_df = spark.createDataFrame(sample_ground_truth, schema=gt_schema)
    gt_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GROUND_TRUTH_TABLE)
    print(f"Inserted {len(sample_ground_truth)} ground truth records.")
else:
    print("No sample ground truth provided yet. Uncomment and populate the list above.")

# COMMAND ----------

display(spark.table(GROUND_TRUTH_TABLE))

# COMMAND ----------

# DBTITLE 1,Run ai_query on ground truth samples and build eval dataset
import mlflow
import json
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Step 1: Read the already-processed batch table output and join with ground
# truth. No need to re-run ai_query() — we evaluate formatted_json_table_data
# directly against ground truth.
# ---------------------------------------------------------------------------

mlflow.set_experiment("/Users/birbal.das@databricks.com/hollyport_capital/final/Evaluate_ai_query_output_exp")

BATCH_TABLE = "serverless_stable_14ey07_catalog.hollyport.all_table_data_batch"

# Read batch results (contains formatted_json_table_data from ai_query + post-processing)
batch_df = spark.table(BATCH_TABLE)
gt_df = spark.table(GROUND_TRUTH_TABLE)

# Join batch output with ground truth on identifying keys
eval_pairs_df = batch_df.join(
    gt_df,
    on=["file_name", "table_id"],
    how="inner"
).select(
    "file_name", "table_id",
    "formatted_json_table_data",  # ARRAY<MAP<STRING,STRING>> — the actual output
    "expected_json",              # STRING — the ground truth JSON array
    # "raw_html_table_data"        # For context in judges
)

eval_count = eval_pairs_df.count()
print(f"Found {eval_count} evaluation pairs (batch output with ground truth).")

if eval_count == 0:
    print("\n⚠️  No ground truth records found. Populate the ground truth table first.")
    dbutils.notebook.exit("No ground truth data available for evaluation.")

# COMMAND ----------

# DBTITLE 1,Invoke ai_query on evaluation subset
# ---------------------------------------------------------------------------
# Step 2: Convert formatted_json_table_data (ARRAY<MAP>) to a JSON string
# for comparison with expected_json ground truth, then collect to pandas.
# ---------------------------------------------------------------------------

# Convert the ARRAY<MAP<STRING,STRING>> to a JSON string for comparison
# This makes it directly comparable to expected_json ground truth
eval_with_json = eval_pairs_df.withColumn(
    "actual_json",
    F.to_json(F.col("formatted_json_table_data"))  # Converts array<map> -> JSON string
)

# Collect to driver for mlflow.genai.evaluate()
eval_results_pdf = eval_with_json.select(
    "file_name", "table_id",
    "actual_json",        # JSON string of actual extraction
    "expected_json",      # JSON string of ground truth
    # "raw_html_table_data", # Context for LLM judges
).toPandas()

print(f"Collected {len(eval_results_pdf)} evaluation results.")
print(f"Records with null actual_json: {eval_results_pdf['actual_json'].isna().sum()}")
display(eval_results_pdf[["file_name", "table_id", "actual_json"]].head(10))

# COMMAND ----------

# DBTITLE 1,Define custom scorers for table extraction evaluation
from mlflow.genai.scorers import scorer
from mlflow.entities import Feedback

# ---------------------------------------------------------------------------
# Custom Scorers: Structural and content-level comparison of JSON outputs
# These compare formatted_json_table_data (actual) vs expected_json (ground truth).
# The "response" field in outputs contains the actual_json string from the batch table.
# ---------------------------------------------------------------------------

@scorer
def exact_match(inputs, outputs, expectations) -> Feedback:
    """Check if actual_json is an exact match to expected_json (after parsing)."""
    try:
        actual = json.loads(outputs.get("response", ""))
        expected = json.loads(expectations.get("expected_response", "[]"))
    except (json.JSONDecodeError, TypeError) as e:
        return Feedback(value=False, rationale=f"JSON parse error: {str(e)[:500]}")

    if actual == expected:
        return Feedback(value=True, rationale="Exact match: actual_json matches expected_json.")
    else:
        # Find first difference for debugging
        diff_detail = ""
        if len(actual) != len(expected):
            diff_detail = f" Row count differs (actual={len(actual)}, expected={len(expected)})."
        else:
            for i, (a_row, e_row) in enumerate(zip(actual, expected)):
                if a_row != e_row:
                    diff_detail = f" First diff at row {i}: actual={str(a_row)[:500]}, expected={str(e_row)[:500]}"
                    break
        return Feedback(value=False, rationale=f"Mismatch: actual_json does NOT match expected_json.{diff_detail}")


@scorer
def record_count_match(inputs, outputs, expectations) -> Feedback:
    """Check if the number of records in actual matches ground truth."""
    try:
        actual = json.loads(outputs.get("response", ""))
        expected = json.loads(expectations.get("expected_response", "[]"))
    except (json.JSONDecodeError, TypeError) as e:
        return Feedback(value=False, rationale=f"JSON parse error: {str(e)[:500]}")

    actual_count = len(actual) if isinstance(actual, list) else 0
    expected_count = len(expected) if isinstance(expected, list) else 0

    if actual_count == expected_count:
        return Feedback(value=True, rationale=f"Record count matches: {expected_count} records.")
    else:
        return Feedback(
            value=False,
            rationale=f"Record count mismatch. Expected: {expected_count}, Actual: {actual_count} (diff: {actual_count - expected_count:+d})."
        )


@scorer
def value_match(inputs, outputs, expectations) -> Feedback:
    """Check if cell values match row-by-row, ignoring key/column names.
    Compares only the ordered list of values in each record."""
    try:
        actual = json.loads(outputs.get("response", ""))
        expected = json.loads(expectations.get("expected_response", "[]"))
    except (json.JSONDecodeError, TypeError) as e:
        return Feedback(value=False, rationale=f"JSON parse error: {str(e)[:500]}")

    if not actual or not expected:
        return Feedback(value=False, rationale="Empty records in actual or expected.")

    total_values = 0
    matching_values = 0
    mismatches = []

    for i in range(min(len(actual), len(expected))):
        actual_vals = list(actual[i].values()) if isinstance(actual[i], dict) else []
        expected_vals = list(expected[i].values()) if isinstance(expected[i], dict) else []

        for j in range(min(len(actual_vals), len(expected_vals))):
            total_values += 1
            if str(actual_vals[j]).strip() == str(expected_vals[j]).strip():
                matching_values += 1
            else:
                if len(mismatches) < 5:
                    mismatches.append(
                        f"Row {i}, pos {j}: expected='{str(expected_vals[j])[:500]}', got='{str(actual_vals[j])[:500]}'"
                    )

        # Count extra values in the longer list as mismatches
        total_values += abs(len(actual_vals) - len(expected_vals))

    # Count values in extra rows (if row counts differ)
    for i in range(min(len(actual), len(expected)), max(len(actual), len(expected))):
        extra_row = actual[i] if i < len(actual) else expected[i]
        total_values += len(extra_row) if isinstance(extra_row, dict) else 0

    accuracy = matching_values / total_values if total_values > 0 else 0.0
    passed = accuracy == 1.0

    rationale = f"Value match: {matching_values}/{total_values} values match ({accuracy:.1%})."
    if mismatches:
        rationale += f" Mismatches: {'; '.join(mismatches)}"

    return Feedback(value=passed, rationale=rationale)


@scorer
def header_match(inputs, outputs, expectations) -> Feedback:
    """Check if the header/key names in actual match those in expected (order-independent)."""
    try:
        actual = json.loads(outputs.get("response", ""))
        expected = json.loads(expectations.get("expected_response", "[]"))
    except (json.JSONDecodeError, TypeError) as e:
        return Feedback(value=False, rationale=f"JSON parse error: {str(e)[:500]}")

    if not actual or not expected:
        return Feedback(value=False, rationale="Empty records in actual or expected.")

    actual_headers = set(actual[0].keys()) if isinstance(actual[0], dict) else set()
    expected_headers = set(expected[0].keys()) if isinstance(expected[0], dict) else set()

    missing = expected_headers - actual_headers
    extra = actual_headers - expected_headers

    if actual_headers == expected_headers:
        return Feedback(
            value=True,
            rationale=f"Headers match: {sorted(expected_headers)}"
        )
    else:
        rationale_parts = []
        if missing:
            rationale_parts.append(f"Missing headers: {sorted(missing)}")
        if extra:
            rationale_parts.append(f"Extra headers: {sorted(extra)}")
        common = actual_headers & expected_headers
        rationale_parts.append(f"Common headers ({len(common)}): {sorted(common)}")
        return Feedback(value=False, rationale="Header mismatch. " + ". ".join(rationale_parts))


print("Custom scorers defined: exact_match, record_count_match, header_count_match, value_match, header_match")

# COMMAND ----------

# DBTITLE 1,Run MLflow evaluation with built-in + custom scorers
from mlflow.genai.scorers import Correctness, Guidelines

eval_dataset = []
for _, row in eval_results_pdf.iterrows():
    if row["actual_json"] is None or str(row["actual_json"]).strip() in ("", "null"):
        continue

    eval_dataset.append({
        "inputs": {
            "request": f"Validate if the response in outputs matches the expected_response"

        },
        "outputs": {
            "response": row["actual_json"],
        },
        "expectations": {
            "expected_response": row["expected_json"]
        },
    })

print(f"Evaluation dataset has {len(eval_dataset)} samples (after filtering null outputs).")

if not eval_dataset:
    print("⚠️  No valid evaluation samples. Check ground truth table and batch output.")
    dbutils.notebook.exit("No valid evaluation samples.")

# COMMAND ----------

# DBTITLE 1,Execute evaluation and display results
with mlflow.start_run(run_name="ai_query_extraction_eval"):
    eval_results = mlflow.genai.evaluate(
        data=eval_dataset,
        scorers=[
            exact_match,
            record_count_match,
            header_match,
            value_match,
        ],
    )

print("\n" + "="*60)
print("EVALUATION COMPLETE")
print("="*60)
print(f"\nMetrics summary:")
for metric_name, metric_value in sorted(eval_results.metrics.items()):
    print(f"  {metric_name}: {metric_value}")