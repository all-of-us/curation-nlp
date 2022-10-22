package org.allofus.curation.pipeline;

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

    public static Schema ReadSchema(String file_path) {
        List<Schema.Field> fields = new LinkedList<>();
        try {
            String project_home = System.getProperty("user.dir");
            String resources_dir = project_home + "/src/main/resources";
            String schema_path = resources_dir + "/schemas/cdm/clinical/";
            byte[] encoded = Files.readAllBytes(Paths.get(schema_path + file_path));
            String file_string = new String(encoded, StandardCharsets.UTF_8);
            JsonArray schema_json_array = JsonParser.parseString(file_string).getAsJsonArray();

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
                    case "date":
                    case "timestamp":
                        datatype = Schema.FieldType.DATETIME;
                        break;
                    default:
                        datatype = Schema.FieldType.STRING;
                }
                fields.add(Schema.Field.nullable(name, datatype));
            }
        } catch (IOException e) {
            System.out.println(e.getMessage() + ", could not read schema for file " + file_path);
        }
        return Schema.of(fields.toArray(new Schema.Field[0]));
    }
}
