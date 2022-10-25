package org.allofus.curation.io.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.allofus.curation.io.factory.IOWrite;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.allofus.curation.utils.ReadSchemaFromJson;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class BigQueryWrite extends IOWrite {

  static BigQuery bigquery;
  static String jsonString = "{ \"fields\": " + ReadSchemaFromJson.getJsonString("note_file") + "}";

  public void init() {
    bigquery = BigQueryOptions.getDefaultInstance().getService();
  }

  @Override
  public PDone expand(PCollection<Row> input) {
    input
        .apply(ParDo.of(new RowToBQ()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format(output_sink))
                .withJsonSchema(jsonString)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .ignoreInsertIds()
                .withTriggeringFrequency(Duration.standardSeconds(5))
                .withNumStorageWriteApiStreams(3)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    return PDone.in(input.getPipeline());
  }

  public static class RowToBQ extends DoFn<Row, TableRow> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<TableRow> receiver) {
      TableRow outputRow =
          new TableRow()
              .set("integer_field", row.getValue("note_nlp_id"))
              .set("integer_field", row.getValue("note_id"))
              .set("integer_field", row.getValue("section_concept_id"))
              .set("string_field", row.getValue("snippet"))
              .set("string_field", row.getValue("offset"))
              .set("string_field", row.getValue("lexical_variant"))
              .set("integer_field", row.getValue("note_nlp_concept_id"))
              .set("integer_field", row.getValue("note_nlp_source_concept_id"))
              .set("string_field", row.getValue("nlp_system"))
              .set("date_field", LocalDate.parse(row.getValue("nlp_date")).toString())
              .set("datetime_field", LocalDateTime.parse(row.getValue("nlp_datetime")).toString())
              .set("string_field", row.getValue("term_exists"))
              .set("string_field", row.getValue("term_temporal"))
              .set("string_field", row.getValue("term_modifiers"));
      receiver.output(outputRow);
    }
  }
}
