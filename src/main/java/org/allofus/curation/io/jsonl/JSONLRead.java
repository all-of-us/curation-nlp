package org.allofus.curation.io.jsonl;

import org.allofus.curation.io.factory.IORead;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public class JSONLRead extends IORead {

  @Override
  public PCollection<Row> expand(PBegin input) {
    return input
        .apply(FileIO.match().filepattern(input_pattern))
        .apply(FileIO.readMatches())
        .apply(ParDo.of(new JSONLReader()))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new JSONLToRow()))
        .setRowSchema(input_schema)
        .setCoder(SchemaCoder.of(input_schema));
  }

  public static class JSONLReader extends DoFn<FileIO.ReadableFile, String> {
    @ProcessElement
    public void processElement(
        @Element FileIO.ReadableFile element, OutputReceiver<String> receiver) throws IOException {
      InputStream is = Channels.newInputStream(element.open());
      BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      reader.lines().forEach(receiver::output);
      reader.close();
    }
  }

  public static class JSONLToRow extends DoFn<String, Row> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<Row> receiver) {
      JSONObject json_obj = new JSONObject(element);
      Row output =
          Row.withSchema(input_schema)
              .addValue(json_obj.optLong("note_id"))
              .addValue(json_obj.optLong("person_id"))
              .addValue(json_obj.optString("note_date"))
              .addValue(json_obj.optString("note_datetime"))
              .addValue(json_obj.optLong("note_type_concept_id"))
              .addValue(json_obj.optLong("note_class_concept_id"))
              .addValue(json_obj.optString("note_title"))
              .addValue(json_obj.optString("note_text"))
              .addValue(json_obj.optLong("encoding_concept_id"))
              .addValue(json_obj.optLong("language_concept_id"))
              .addValue(json_obj.optLong("provider_id"))
              .addValue(json_obj.optLong("visit_occurrence_id"))
              .addValue(json_obj.optLong("visit_detail_id"))
              .addValue(json_obj.optString("note_source_value"))
              .build();
      receiver.output(output);
    }
  }
}
