package org.allofus.curation.io.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.allofus.curation.io.factory.IOWrite;
import org.allofus.curation.utils.ReadSchemaFromJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STORAGE_WRITE_API;

public class BigQueryWrite extends IOWrite {

  static BigQuery bigquery;
  static String jsonString =
      "{ \"fields\": " + ReadSchemaFromJson.getJsonString("note_nlp.json") + "}";

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
                .ignoreInsertIds()
//                .withMethod(STORAGE_WRITE_API)
//                .withTriggeringFrequency(Duration.standardMinutes(3))
                .withNumStorageWriteApiStreams(10)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    return PDone.in(input.getPipeline());
  }

  public class RowToBQ extends DoFn<Row, TableRow> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<TableRow> receiver) {
      TableRow outputRow = new TableRow();
      for (Schema.Field field : output_schema.getFields()) {
        outputRow.set(field.getName(), row.getValue(field.getName()));
      }
      receiver.output(outputRow);
    }
  }
}
