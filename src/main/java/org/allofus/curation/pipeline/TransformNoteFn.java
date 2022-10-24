package org.allofus.curation.pipeline;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import utils.ReadSchemaFromJson;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TransformNoteFn extends DoFn<Row, Row> {
  @ProcessElement
  public void processElement(@Element Row input, OutputReceiver<Row> receiver) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = new Date();
    Schema output_schema = ReadSchemaFromJson.ReadSchema("note_nlp.json");
    Row output =
        Row.withSchema(output_schema)
            .addValue(0)
            .addValue(input.getValue("note_id"))
            .addValue(0)
            .addValue("test_snippet")
            .addValue("0-100")
            .addValue("test_variant")
            .addValue(100)
            .addValue(100)
            .addValue("CLAMP 1.7.2")
            .addValue(dateFormat.format(date))
            .addValue(datetimeFormat.format(date))
            .addValue("False")
            .addValue("1 year")
            .addValue("TermModifier")
            .build();
    receiver.output(output);
  }
}
