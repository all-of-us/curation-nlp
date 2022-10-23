package io.factory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import utils.ReadSchemaFromJson;

public abstract class IOWrite {

    public String output_pattern;
    public Schema output_schema = ReadSchemaFromJson.ReadSchema("note_nlp.json");

    public void init(String output_dir, String output_type) {
        if ("bigquery".equalsIgnoreCase(output_type)){
            output_pattern = output_dir;
        } else {
            output_pattern = output_dir + ".*" + output_type;
        }
    }
    public abstract void expand(PCollection<Row> input);
}
