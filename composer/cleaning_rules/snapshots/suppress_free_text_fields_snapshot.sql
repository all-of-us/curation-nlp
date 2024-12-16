{% snapshot suppress_free_text_fields_snapshot %}

{{
    config(
        target_database= var('dbt_project_id'),
        target_schema= var('dbt_dataset_id'),
        unique_key='note_nlp_id',
        strategy='check',
        check_cols = ['nlp_date', 'nlp_datetime'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    *
FROM {{ ref('suppress_free_text_fields') }}

{% endsnapshot %}