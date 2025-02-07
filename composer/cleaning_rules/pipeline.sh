sh delete_snapshots.sh \
    && dbt run --exclude "post_snapshot" \
    && dbt snapshot \
    && sh progress_models.sh \
    && dbt snapshot \
    && dbt run --select post_snapshot