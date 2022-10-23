package io.jsonl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.factory.IORead;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

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
      JsonObject json_obj = JsonParser.parseString(element).getAsJsonObject();
      Row output =
          Row.withSchema(input_schema)
              .addValue(json_obj.get("note_id").getAsLong())
              .addValue(json_obj.get("person_id").getAsLong())
              .addValue(json_obj.get("note_date").getAsString())
              .addValue(json_obj.get("note_datetime").getAsString())
              .addValue(json_obj.get("note_type_concept_id").getAsLong())
              .addValue(json_obj.get("note_class_concept_id").getAsLong())
              .addValue(json_obj.get("note_title").getAsString())
              .addValue(json_obj.get("note_text").getAsString())
              .addValue(json_obj.get("encoding_concept_id").getAsLong())
              .addValue(json_obj.get("language_concept_id").getAsLong())
              .addValue(json_obj.get("provider_id").getAsLong())
              .addValue(json_obj.get("visit_occurrence_id").getAsLong())
              .addValue(json_obj.get("visit_detail_id").getAsLong())
              .addValue(json_obj.get("note_source_value").getAsString())
              .build();
      receiver.output(output);
    }
  }
}
