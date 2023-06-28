package org.allofus.curation.io.parquet;

import org.allofus.curation.io.factory.IORead;
import org.allofus.curation.utils.ReadSchemaFromAvro;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRead extends IORead {
  static final Logger LOG = LoggerFactory.getLogger(ParquetRead.class);

  static org.apache.avro.Schema avroSchema = ReadSchemaFromAvro.ReadSchema("note.avsc");

  @Override
  public PCollection<Row> expand(PBegin input) {
    return input
        .apply(FileIO.match().filepattern(input_pattern))
        .apply(FileIO.readMatches())
        .apply(ParquetIO.readFiles(avroSchema))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new ParquetRead.ParquetToRow()))
        .setRowSchema(input_schema)
        .setCoder(SchemaCoder.of(input_schema));
  }

  public static class ParquetToRow extends DoFn<GenericRecord, Row> {
    @ProcessElement
    public void processElement(@Element GenericRecord element, OutputReceiver<Row> receiver) {
      receiver.output(AvroUtils.toBeamRowStrict(element, input_schema));
    }
  }
}
