# Define your list of checks (table names)
checks=("note_nlp" "remove_rows_without_standard_concept" "remove_non_condition_rows" "suppress_free_text_fields" "reset_nlp_dates")

dataset_id=$DBT_DATASET_ID


# Iterate over each check except the last one
for ((i=0; i<${#checks[@]}-1; i++)); do
  current_table=${checks[$i]}
  next_table=${checks[$i+1]}

  # Use the BigQuery command line tool to copy the next table into the current one
  echo "Overwriting $current_table with contents of $next_table"
  # echo "bq cp --force=true '$project_id.$dataset_id.$next_table' '$project_id.$dataset_id.$current_table'"
  bq cp --force=true "$dataset_id.$next_table" "$dataset_id.$current_table"

  if [ $? -ne 0 ]; then
    echo "Failed to overwrite $current_table with $next_table"
    exit 1
  fi
done