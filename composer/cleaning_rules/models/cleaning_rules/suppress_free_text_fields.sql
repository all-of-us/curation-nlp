
{{ config(materialized='table') }}

SELECT
    *
    REPLACE(
        '' AS snippet,
        '' AS offset,
        '' AS lexical_variant,
        '' AS term_temporal,
        '' AS term_modifiers
    )
FROM {{ ref('remove_non_condition_rows') }}