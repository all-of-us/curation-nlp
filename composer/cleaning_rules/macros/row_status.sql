{% macro row_status(table, applied_cleaning_rule, ord) %}
  SELECT
    t.note_nlp_id, 1 ord,
    [STRUCT({{"'" ~  applied_cleaning_rule  ~ "'"}} AS name,
    IF(snap.note_nlp_id IS NOT NULL, 'UPDATE', 'DELETE') AS state )] cleaning_rule
  FROM {{ ref(table ~ "_snapshot") }} t
  LEFT JOIN {{ ref(table) }} snap
    ON snap.note_nlp_id = t.note_nlp_id
  WHERE dbt_valid_to IS NOT NULL

{% endmacro %}