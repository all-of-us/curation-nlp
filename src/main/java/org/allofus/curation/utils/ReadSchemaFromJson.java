package org.allofus.curation.utils;

import org.allofus.curation.pipeline.RunCLAMPFn;
import org.apache.beam.sdk.schemas.Schema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static org.allofus.curation.utils.Constants.ProjectPaths.SCHEMA_CLINICAL;

public class ReadSchemaFromJson {

  private static final Logger LOG = LoggerFactory.getLogger(ReadSchemaFromJson.class);

  public static Schema ReadSchema(String file_path) {
    List<Schema.Field> fields = new LinkedList<>();
    try {
      byte[] encoded = Files.readAllBytes(Paths.get(SCHEMA_CLINICAL + "/" + file_path));
      String file_string = new String(encoded, StandardCharsets.UTF_8);
      JSONArray schema_json_array = new JSONArray(file_string);

      for (Object schema_json_field : schema_json_array) {
        JSONObject field = (JSONObject) schema_json_field;

        String name = field.optString("name");
        String type = field.optString("type");
        Schema.FieldType datatype;
        switch (type.toLowerCase()) {
          case "integer":
            datatype = Schema.FieldType.INT64;
            break;
          case "float":
            datatype = Schema.FieldType.DOUBLE;
            break;
          default:
            datatype = Schema.FieldType.STRING;
        }
        fields.add(Schema.Field.nullable(name, datatype));
      }
    } catch (IOException e) {
      LOG.info(e.getMessage() + ", could not read schema for file " + file_path);
    }
    return Schema.of(fields.toArray(new Schema.Field[0]));
  }

  public static String getJsonString(String file_path) {

    try {
      byte[] encoded = Files.readAllBytes(Paths.get(SCHEMA_CLINICAL + "/" + file_path));
      return new String(encoded, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.info(e.getMessage() + ", could not read schema for file " + file_path);
      return "";
    }
  }
}
