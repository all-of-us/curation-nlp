
{{ config(materialized='table') }}

SELECT
    *
    REPLACE(
        PARSE_DATE('%Y-%m-%d', '{{ var('ehr_cutoff_date') }}') AS nlp_date,
        PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('ehr_cutoff_date') }}') AS nlp_datetime
    )
FROM {{ ref('suppress_free_text_fields') }}