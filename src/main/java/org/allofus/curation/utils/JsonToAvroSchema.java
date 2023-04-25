package org.allofus.curation.utils;
import org.apache.avro.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class JsonToAvroSchema {

  private static final Map<String, String> JSON_AVRO_MAP;

  static {
    JSON_AVRO_MAP = new HashMap<>();
    JSON_AVRO_MAP.put("string", "string");
    JSON_AVRO_MAP.put("number", "double");
    JSON_AVRO_MAP.put("integer", "int");
    JSON_AVRO_MAP.put("boolean", "boolean");
    JSON_AVRO_MAP.put("null", "null");
    JSON_AVRO_MAP.put("array", "array");
    JSON_AVRO_MAP.put("object", "record");
  }

  private static String convertToAvroType(String jsonType) {
    return JSON_AVRO_MAP.getOrDefault(jsonType, "string");
  }

  private static JSONArray convertProperties(JSONArray jsonArray) {
    JSONArray avroFields = new JSONArray();
    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject property = jsonArray.getJSONObject(i);
      JSONObject avroField = new JSONObject();
      avroField.put("name", property.getString("name"));
      avroField.put("type", convertToAvroType(property.getString("type")));

      if (property.has("items")) {
        JSONObject avroFieldType = new JSONObject();
        avroFieldType.put("type", "array");
        avroFieldType.put("items", convertToAvroType(property.getJSONObject("items").getString("type")));
        avroField.put("type", avroFieldType);
      }

      avroFields.put(avroField);
    }
    return avroFields;
  }

  public static Schema getAvroSchema(String JsonSchemaString) {
    JSONArray jsonArray = new JSONArray(JsonSchemaString);

    JSONObject avroSchema = new JSONObject();
    avroSchema.put("type", "record");
    avroSchema.put("name", "Root");
    avroSchema.put("fields", convertProperties(jsonArray));

    Schema.Parser parser = new Schema.Parser();
    return parser.parse(avroSchema.toString());
  }
}
