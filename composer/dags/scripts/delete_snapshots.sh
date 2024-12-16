snapshots=(
    "note_nlp_snapshot" "remove_rows_without_standard_concept_snapshot" "remove_non_condition_rows_snapshot" "suppress_free_text_fields_snapshot" "reset_nlp_dates_snapshot"
)
# dataset_id=$DBT_DATASET_ID
dataset_id=$1

for ((i=0; i<${#snapshots[@]}; i++)); do
  current_table=${snapshots[$i]}
  bq rm -f -t "$dataset_id.$current_table"
done