package io.factory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import utils.ReadSchemaFromJson;

public abstract class IOWrite extends PTransform<PCollection<Row>, PDone> {

  public String output_sink;
  public String output_ext;
  public Schema output_schema = ReadSchemaFromJson.ReadSchema("note_nlp.json");

  public void init(String output_dir, String output_type) {
    if ("bigquery".equalsIgnoreCase(output_type)) {
      output_sink = output_dir;
    } else {
      output_sink = output_dir;
      output_ext = output_type;
    }
  }

  public abstract PDone expand(PCollection<Row> input);
}
