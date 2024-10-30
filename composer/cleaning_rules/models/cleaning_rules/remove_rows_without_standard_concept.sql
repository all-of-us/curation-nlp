
{{ config(materialized='table') }}

SELECT
    n.*
FROM {{ ref('note_nlp') }} n
LEFT JOIN {{ source('source', 'concept') }} c
    ON n.note_nlp_concept_id = c.concept_id
WHERE c.concept_id IS NOT NULL and n.note_nlp_concept_id IS NOT NULL
    AND n.note_nlp_concept_id <> 0
