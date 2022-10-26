package org.allofus.curation.io.factory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.allofus.curation.utils.ReadSchemaFromJson;

public abstract class IORead extends PTransform<PBegin, PCollection<Row>> {

  public static Schema input_schema = ReadSchemaFromJson.ReadSchema("note.json");
  public String input_pattern;

  public void init(String input_dir, String input_type) {
    if ("bigquery".equalsIgnoreCase(input_type)) {
      input_pattern = input_dir;
    } else {
      input_pattern = input_dir + "/*" + input_type;
    }
  }

  public abstract PCollection<Row> expand(PBegin input);
}
