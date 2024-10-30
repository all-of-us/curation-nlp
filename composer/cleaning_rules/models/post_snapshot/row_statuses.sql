
{{ config(materialized='view') }}

WITH all_snapshots AS (
  {{ row_status('note_nlp', 'remove_rows_without_standard_concept', 1) }}
  UNION ALL
  {{ row_status('remove_rows_without_standard_concept', 'remove_non_condition_rows', 2) }}
  UNION ALL
  {{ row_status('remove_non_condition_rows', 'suppress_free_text_fields', 3) }}
  UNION ALL
  {{ row_status('suppress_free_text_fields', 'reset_nlp_dates', 4) }}
)
SELECT
  note_nlp_id, ARRAY_CONCAT_AGG(cleaning_rule ORDER BY ord) cleaning_rules
FROM all_snapshots
GROUP BY note_nlp_id
