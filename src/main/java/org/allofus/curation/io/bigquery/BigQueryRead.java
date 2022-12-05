package org.allofus.curation.io.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.allofus.curation.io.factory.IORead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.math.NumberUtils;

public class BigQueryRead extends IORead {

  static BigQuery bigquery;

  public void init() {
    bigquery = BigQueryOptions.getDefaultInstance().getService();
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    return input
        .apply(
            BigQueryIO.readTableRows()
                .fromQuery(String.format("SELECT * FROM " + input_pattern))
                .usingStandardSql())
        .apply(ParDo.of(new BQToRow()));
  }

  public static class BQToRow extends DoFn<TableRow, Row> {
    @ProcessElement
    public void processElement(@Element TableRow tableRow, OutputReceiver<Row> receiver) {
      Row output =
          Row.withSchema(input_schema)
              .addValue(NumberUtils.toLong(tableRow.get("note_id").toString()))
              .addValue(NumberUtils.toLong(tableRow.get("person_id").toString()))
              .addValue(tableRow.get("note_date"))
              .addValue(tableRow.get("note_datetime"))
              .addValue(NumberUtils.toLong(tableRow.get("note_type_concept_id").toString()))
              .addValue(NumberUtils.toLong(tableRow.get("note_class_concept_id").toString()))
              .addValue(tableRow.get("note_title"))
              .addValue(tableRow.get("note_text"))
              .addValue(NumberUtils.toLong(tableRow.get("encoding_concept_id").toString()))
              .addValue(NumberUtils.toLong(tableRow.get("language_concept_id").toString()))
              .addValue(NumberUtils.toLong(tableRow.get("provider_id").toString()))
              .addValue(NumberUtils.toLong(tableRow.get("visit_occurrence_id").toString()))
              .addValue(NumberUtils.toLong(tableRow.get("visit_detail_id").toString()))
              .addValue(tableRow.get("note_source_value"))
              .build();
      receiver.output(output);
    }
  }
}
