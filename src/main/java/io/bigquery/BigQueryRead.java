package io.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import io.factory.IORead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BigQueryRead extends IORead {

  @Override
  public PCollection<Row> expand(PBegin input) {
    return input
        .apply(BigQueryIO.readTableRows().from(input_pattern))
        .apply(ParDo.of(new BQToRow()));
  }

  public static class BQToRow extends DoFn<TableRow, Row> {
    @ProcessElement
    public void processElement(@Element TableRow tableRow, OutputReceiver<Row> receiver) {
      Row output =
          Row.withSchema(input_schema)
              .addValue(tableRow.get("note_id"))
              .addValue(tableRow.get("person_id"))
              .addValue(tableRow.get("note_date"))
              .addValue(tableRow.get("note_datetime"))
              .addValue(tableRow.get("note_type_concept_id"))
              .addValue(tableRow.get("note_class_concept_id"))
              .addValue(tableRow.get("note_title"))
              .addValue(tableRow.get("note_text"))
              .addValue(tableRow.get("encoding_concept_id"))
              .addValue(tableRow.get("language_concept_id"))
              .addValue(tableRow.get("provider_id"))
              .addValue(tableRow.get("visit_occurrence_id"))
              .addValue(tableRow.get("visit_detail_id"))
              .addValue(tableRow.get("note_source_value"))
              .build();
      receiver.output(output);
    }
  }
}
