package org.allofus.curation.io.parquet;

import com.google.cloud.bigquery.BigQuery;
import org.allofus.curation.io.factory.IOWrite;
import org.allofus.curation.utils.JsonToAvroSchema;
import org.allofus.curation.utils.ReadSchemaFromJson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class ParquetWrite extends IOWrite {

  static BigQuery bigquery;
  static String jsonString =
      "{ \"fields\": " + ReadSchemaFromJson.getJsonString("note_nlp.json") + "}";

  static Schema avroSchema = JsonToAvroSchema.getAvroSchema(jsonString);

  @Override
  public PDone expand(PCollection<Row> input) {
    input
      .apply(ParDo.of(new ParquetWrite.RowToParquet()))
      .apply("Windowing",
        Window.<GenericRecord>into(FixedWindows.of(Duration.standardSeconds(output_partition_seconds)))
          .triggering(Repeatedly.forever(
            AfterFirst.of(AfterPane.elementCountAtLeast(output_batch_size),
              AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(output_partition_seconds/4)))))
          .withAllowedLateness(Duration.standardSeconds(output_partition_seconds/12))
          .discardingFiredPanes())
      .apply(
          FileIO.<GenericRecord>write()
              .via(ParquetIO.sink(avroSchema))
              .to(output_sink)
              .withSuffix("." + output_ext));
    return PDone.in(input.getPipeline());
  }

  public static class RowToParquet extends DoFn<Row, GenericRecord> {
    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<GenericRecord> receiver) {
      receiver.output(AvroUtils.toGenericRecord(element, avroSchema));
    }
  }
}
