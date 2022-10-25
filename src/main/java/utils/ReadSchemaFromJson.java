package utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.schemas.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class ReadSchemaFromJson {

  static String project_home = System.getProperty("user.dir");
  static String resources_dir = project_home + "/src/main/resources";
  static String schema_path = resources_dir + "/schemas/cdm/clinical/";

  public static Schema ReadSchema(String file_path) {
    List<Schema.Field> fields = new LinkedList<>();
    String jsonString = getJsonString(schema_path + file_path);
    JsonArray schema_json_array = JsonParser.parseString(jsonString).getAsJsonArray();

    for (Object schema_json_field : schema_json_array) {
      JsonObject field = (JsonObject) schema_json_field;

      String name = field.get("name").getAsString();
      String type = field.get("type").getAsString();
      Schema.FieldType datatype;
      switch (type) {
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
    return Schema.of(fields.toArray(new Schema.Field[0]));
  }

  public static String getJsonString(String file_path) {
    byte[] encoded = new byte[0];
    try {
      encoded = Files.readAllBytes(Paths.get(schema_path + file_path));
    } catch (IOException e) {
      System.out.println(e.getMessage() + ", could not read schema for file " + file_path);
    }
    return new String(encoded, StandardCharsets.UTF_8);
  }
}
