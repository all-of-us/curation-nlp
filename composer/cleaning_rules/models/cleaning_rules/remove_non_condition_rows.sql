
{{ config(materialized='table') }}

SELECT
    n.*
FROM {{ ref('remove_rows_without_standard_concept') }} n
JOIN {{ source('source', 'concept') }} c
    ON n.note_nlp_concept_id = c.concept_id
WHERE c.domain_id = 'Condition'
