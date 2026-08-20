"""Utility functions and prompt template for PDF table extraction."""

import re


def fix_misaligned_headers(records, table_columns):
    """When a table has empty <th> headers (assigned column1_llm, column2_llm, etc.)
    and the first data row fills those positions while other columns are empty,
    promote that first row's values as column names and remove it."""
    if not records or not table_columns:
        return records

    generic_pattern = re.compile(r'^column\d+_llm$')
    first_row = records[0]

    # Find generic-named keys with a non-empty value in the first row
    rename_map = {}
    for k, v in first_row.items():
        if generic_pattern.match(k) and v and str(v).strip():
            rename_map[k] = str(v).strip()

    if not rename_map:
        return records

    # Confirm it's a header row: non-generic columns must be empty
    for k, v in first_row.items():
        if k not in rename_map and v and str(v).strip():
            return records  # has data in other columns - not a misaligned header

    # Rename keys in all remaining records and drop the first row
    new_records = []
    for rec in records[1:]:
        new_rec = {}
        for k, v in rec.items():
            new_rec[rename_map.get(k, k)] = v
        new_records.append(new_rec)

    return new_records


def merge_partial_rows(records):
    """Deterministically merge consecutive partial rows into the next full row.
    A partial row = all columns empty/null except one (a multi-line description)."""
    if not records or len(records[0]) <= 1:
        return records

    merged = []
    pending_values = []
    pending_col = None

    for rec in records:
        non_empty = {k: v for k, v in rec.items() if v and str(v).strip()}

        if len(non_empty) <= 1 and len(rec) > 1:
            if non_empty:
                col, val = next(iter(non_empty.items()))
                if pending_col is None:
                    pending_col = col
                pending_values.append(val)
        else:
            if pending_values and pending_col:
                existing = rec.get(pending_col, "")
                if existing and str(existing).strip():
                    pending_values.append(existing)
                rec[pending_col] = "\n".join(pending_values)
            pending_values = []
            pending_col = None
            merged.append(rec)

    if pending_values and pending_col:
        row = {k: "" for k in records[0].keys()}
        row[pending_col] = "\n".join(pending_values)
        merged.append(row)

    return merged


def sanitize_table_name(file_name, element_id):
    """Convert file name + element_id to a valid Delta table name."""
    # Remove .pdf extension
    name = file_name.rsplit('.', 1)[0] if file_name.lower().endswith('.pdf') else file_name
    # Replace all invalid characters (spaces, hyphens, periods, slashes, control chars) with underscores
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Append element_id
    name = f"{name}_{element_id}"
    return name



def get_extraction_prompt(raw_html):
    """Return the LLM prompt for parsing an HTML table into JSON records."""
    return f"""You are a data extraction expert. Parse the following HTML table into a JSON array of records.

            IMPORTANT RULES:

            1. It is normal that some tables may not have any header in html data. If so, assign it a name like column1_llm, column2_llm, column3_llm and so on.
            2. When one column contains descriptive values across multiple consecutive rows while all other columns in those rows are empty/null, this indicates a multi-line description split across rows. Merge those rows (may be two or more rows as required) by concatenating the descriptive values (joined by "\n") into the corresponding column of the next row that has values in multiple columns, and remove the partial rows. Example: ["A","",""], ["B","",""], ["C","$100","$200"] -> ["A\nB\nC","$100","$200"].
            3. If the HTML has colspan="2" on any header - treat it as a SINGLE column.
            4. Keep numeric values as strings (preserve commas and $ signs).
            5. Return ONLY a valid JSON array, no markdown, no explanation.
            6. Keep the JSON columns in the same order as they appear in the html table.
            7. The currency symbol like $, etc sometimes is far apart from the number. As such, they may appear as separate columns in html data. In such case, merge them into a single column.
            8. Keep the output JSON always in valid format.
            9. If a table row contains multiple values separated by <br> tags, split it into separate JSON records, with each <br>-separated value becoming its own record. Do not duplicate columns that contain only a single-line value; leave those fields empty in the additional records unless the column itself contains value in multiple lines.
            10. If any header column (<th>) in the HTML is empty or blank, assign it a name like column1_llm, column2_llm, column3_llm and so on. Never use an empty string as a JSON key.


            HTML Table:
            {raw_html}"""
