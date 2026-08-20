# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A **Databricks pipeline** that extracts tabular data from PDF financial statements
(private equity fund reports, e.g. "Bain X", "Permira V") into structured JSON/Delta
tables. Every `.py` file is a **Databricks notebook**, not a standalone script:

- Cells are delimited by `# COMMAND ----------`; `# MAGIC %sql` / `# MAGIC %pip` lines are cell magics.
- `spark`, `dbutils`, and `display()` are injected by the Databricks runtime — there is no
  `main()`, no `SparkSession.builder`, and the notebooks cannot run under plain `python`.
- The `# /// script` header pins **serverless environment version 5**.

There is **no local build/lint/test tooling** (no `requirements.txt`, `pyproject.toml`, or
test suite). Notebooks are run cell-by-cell in a Databricks workspace, or deployed as a
Lakeflow job via the Databricks Asset Bundle (see below).

Each notebook reads its config from `dbutils.widgets` (`catalog`, `schema`, `volume`,
`source_subpath`, `model`, `max_tokens`, `prompt`, `util_path`) with defaults that match the
original hardcoded values, so they still run standalone. Under the job, the bundle injects
these as job parameters. Table names are built with a `t("name")` helper = `catalog.schema.name`.

## Pipeline stages (run in this order)

The numeric suffix in each filename is its stage order. All data lives in the Unity Catalog
schema **`serverless_stable_14ey07_catalog.hollyport`** and PDFs are read from
`/Volumes/serverless_stable_14ey07_catalog/hollyport/vol1/sources/...`.

| Order | Notebook | Reads | Writes |
|-------|----------|-------|--------|
| 01 | `src/Raw_parse_task-01.py` | PDFs (via `READ_FILES` + `ai_parse_document`) | `raw_parsed` (all exploded elements) |
| 02 | `src/Extract_task-02.py` | `raw_parsed` | `parsed_table_elements_staging` → `all_table_data_batch` → `extracted_table_data_variant` |
| 02.1 | `src/Extract_table_metadata_task-02.1.py` | `raw_parsed` | `table_metadata` → `grouped_table_metadata` |
| 03 | `src/merge_multipage_table-03.py` | `grouped_table_metadata` + `extracted_table_data_variant` | `extracted_table_data_variant_merged` (final output) |

`util/` holds supporting notebooks/modules (not part of the linear DAG):
- `pdf_table_utils.py` — plain importable module with the post-processing functions.
- `format_table_output_to_json.py` — reconstructs an ordered JSON array for one `(file_name, table_id)`.
- `Evaluate_ai_query_output.py` — MLflow GenAI evaluation against a manual ground-truth table.

## Deploying as a Lakeflow job (DAB)

The bundle wires the four stages into one job with the DAG
`01 → (02, 02.1) → 03` (task#4 depends on both task#2 and task#3), on **serverless** compute.

- `databricks.yml` — bundle name, `include`s, and the `dev`/`stage`/`prod` targets. All three
  deploy through the **`fevm`** CLI profile; each overrides only `catalog`
  (`serverless_stable_14ey07_catalog_{dev,stage,prod}`). `schema` stays `hollyport`.
- `variables.yml` — **single source of truth for all config**: catalog, schema, volume,
  `source_subpath`, model, max_tokens, and the full extraction `prompt`. These become job
  parameters (see `resources/`).
- `resources/hollyport_pipeline.job.yml` — the job: tasks, `depends_on` edges, and job-level
  `parameters` sourced from `${var.*}`, plus `util_path: ${workspace.file_path}/util` so the
  notebooks can `import pdf_table_utils`.

```bash
databricks auth login -p fevm            # refresh the fevm token first
databricks bundle validate -t dev
databricks bundle deploy   -t dev        # or -t stage / -t prod
databricks bundle run hollyport_table_extraction -t dev
```

## How extraction works (the core of stage 02)

1. Table elements (`elem.type == 'table'`) are pulled from `raw_parsed` as raw HTML and
   assigned a `table_id` via `dense_rank()` over `(file_name, file_location)` ordered by `element_id`.
2. `ai_query(...)` is called **inside a SQL expression** so the LLM runs distributed across
   executors — Databricks handles retries, rate-limiting, and endpoint scaling. `failOnError => false`
   returns a `struct(result, errorMessage)` for graceful failure handling.
3. The `post_process_table` **UDF** parses the LLM's JSON (with a fallback that scans for the
   last valid `[...]` array) and applies deterministic cleanup from `pdf_table_utils`:
   `fix_misaligned_headers` (promotes a spurious first data row into column names) and
   `merge_partial_rows` (folds multi-line description rows into the next full row).
4. Results are exploded to one row per record and stored as a Spark **VARIANT** column using
   the recurring idiom `parse_json(to_json(named_struct(...)))`.

## Things to know before editing

- **The prompt lives in `variables.yml` for job runs.** `Extract_task-02.py` still holds an
  inline `PROMPT_TEMPLATE` as a **standalone fallback**, but the `prompt` job parameter (from
  `variables.yml`) overrides it at runtime (`if prompt_param.strip(): PROMPT_TEMPLATE = prompt_param`).
  Edit the prompt in `variables.yml` for deployed behavior; keep the two in sync if you rely on
  standalone runs. A third, older/shorter variant (`get_extraction_prompt`) sits unused in
  `util/pdf_table_utils.py`.
- **`util/` must be importable.** `Extract_task-02.py` does `from pdf_table_utils import ...`.
  The job passes `util_path=${workspace.file_path}/util` and the notebook `sys.path.append`s it
  before the import; standalone runs rely on `util/` already being on the path.
- **The LLM model is a `variables.yml` knob** (`model`, default `databricks-gpt-oss-120b`);
  alternatives (`databricks-claude-sonnet-4-6`, `gpt-oss-20b`, `qwen3-next-80b-a3b-instruct`)
  are noted inline in `Extract_task-02.py`.
- **Source PDFs** are located by `/Volumes/{catalog}/{schema}/{volume}/{source_subpath}`, all
  four parts being variables; `source_subpath` defaults to `sources/hollyport_test/*/*.pdf`.
- **Interactive `%sql`/preview cells are disabled/guarded for job safety.** The trailing
  `SELECT * FROM <hardcoded catalog>` cells are commented out (they'd hit the wrong catalog per
  env) and the one-off preview in `merge_multipage_table-03.py` is guarded with `if columns_list:`.
- **Multi-page ("continued") table merging** (stage 03) relies on stage 02.1: tables sharing a
  normalized `nearest_page_header`/`section_header`/`title` are grouped, and rows are re-keyed
  to the group's `min(table_id)` — but only when every member has identical `formatted_table_columns`.
  The `continued_pattern` regex in `Extract_table_metadata_task-02.1.py` strips trailing
  "(continued)"-style suffixes so a split table normalizes to one group.
- **Evaluation is offline.** `Evaluate_ai_query_output.py` joins `all_table_data_batch` against
  the hand-populated `extraction_ground_truth` table and scores with custom MLflow scorers
  (`exact_match`, `record_count_match`, `value_match`, `header_match`) — it does not re-run `ai_query`.
- All writes use `.mode("overwrite").option("overwriteSchema", "true")` — reruns fully replace
  their target tables.
