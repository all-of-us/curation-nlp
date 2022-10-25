package org.allofus.curation.io.csv;

import org.allofus.curation.io.factory.IORead;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

import utils.GetLongValue;

public class CSVRead extends IORead {

  public PCollection<Row> expand(PBegin input) {
    return input
        .apply(FileIO.match().filepattern(input_pattern))
        .apply(FileIO.readMatches())
        .apply(ParDo.of(new CSVReader()))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new CSVToRow()))
        .setRowSchema(input_schema)
        .setCoder(SchemaCoder.of(input_schema));
  }

  public static class CSVReader extends DoFn<FileIO.ReadableFile, CSVRecord> {
    @ProcessElement
    public void processElement(
        @Element FileIO.ReadableFile element, OutputReceiver<CSVRecord> receiver)
        throws IOException {
      InputStream is = Channels.newInputStream(element.open());
      Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
      Iterable<CSVRecord> records =
          CSVFormat.DEFAULT.withHeader().withDelimiter(',').withFirstRecordAsHeader().parse(reader);
      for (CSVRecord record : records) {
        receiver.output(record);
      }
    }
  }

  public static class CSVToRow extends DoFn<CSVRecord, Row> {
    @ProcessElement
    public void processElement(@Element CSVRecord element, OutputReceiver<Row> receiver) {
      Row output =
          Row.withSchema(input_schema)
              .addValue(GetLongValue.of(element.get(0)))
              .addValue(GetLongValue.of(element.get(1)))
              .addValue(element.get(2))
              .addValue(element.get(3))
              .addValue(GetLongValue.of(element.get(4)))
              .addValue(GetLongValue.of(element.get(5)))
              .addValue(element.get(6))
              .addValue(element.get(7))
              .addValue(GetLongValue.of(element.get(8)))
              .addValue(GetLongValue.of(element.get(9)))
              .addValue(GetLongValue.of(element.get(10)))
              .addValue(GetLongValue.of(element.get(11)))
              .addValue(GetLongValue.of(element.get(12)))
              .addValue(element.get(13))
              .build();
      receiver.output(output);
    }
  }
}