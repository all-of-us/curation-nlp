{% snapshot remove_non_condition_rows_snapshot %}

{{
    config(
        target_database= var('dbt_project_id'),
        target_schema= var('dbt_dataset_id'),
        unique_key='note_nlp_id',
        strategy='check',
        check_cols = 'all',
        invalidate_hard_deletes=True
    )
}}

SELECT
    *
FROM {{ ref('remove_non_condition_rows') }}

{% endsnapshot %}