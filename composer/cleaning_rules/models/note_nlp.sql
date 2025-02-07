
{{ config(materialized='table') }}

WITH note_nlp AS (
    SELECT
    *, CAST(
        CONCAT(
        LPAD(CAST(note_id AS STRING), 9, '0'),
        LPAD(CAST (
            ROW_NUMBER() OVER(PARTITION BY note_id ORDER BY note_id, CAST(SPLIT(offset, '-')[0] AS int) )
            AS STRING
        ), 5, '0')
    )
     AS INT64) AS potential_note_nlp_id
    FROM {{ source('source', var('note_nlp_table_name')) }}
)
SELECT
    *
    EXCEPT(potential_note_nlp_id)
    REPLACE(potential_note_nlp_id AS note_nlp_id)
    
FROM note_nlp