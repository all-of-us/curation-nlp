package org.allofus.curation.utils;

import junit.framework.TestCase;
import org.allofus.curation.utils.ReadSchemaFromJson;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Rule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static com.shazam.shazamcrest.MatcherAssert.assertThat;
import static com.shazam.shazamcrest.matcher.Matchers.sameBeanAs;

public class ReadSchemaFromJsonTest extends TestCase {

  @Rule String project_home = System.getProperty("user.dir");
  @Rule String resources_dir = project_home + "/src/main/resources";
  @Rule String schema_path = resources_dir + "/schemas/cdm/clinical/";

  @Rule
  static Schema getNoteSchema() {
    List<Schema.Field> FIELDS = new LinkedList<>();
    FIELDS.add(Schema.Field.nullable("note_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("person_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("note_date", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("note_datetime", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("note_type_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("note_class_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("note_title", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("note_text", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("encoding_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("language_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("provider_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("visit_occurrence_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("visit_detail_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("note_source_value", Schema.FieldType.STRING));
    return Schema.of(FIELDS.toArray(new Schema.Field[0]));
  }

  @Rule
  static Schema getNoteNLPSchema() {
    List<Schema.Field> FIELDS = new LinkedList<>();
    FIELDS.add(Schema.Field.nullable("note_nlp_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("note_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("section_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("snippet", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("offset", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("lexical_variant", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("note_nlp_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("note_nlp_source_concept_id", Schema.FieldType.INT64));
    FIELDS.add(Schema.Field.nullable("nlp_system", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("nlp_date", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("nlp_datetime", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("term_exists", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("term_temporal", Schema.FieldType.STRING));
    FIELDS.add(Schema.Field.nullable("term_modifiers", Schema.FieldType.STRING));
    return Schema.of(FIELDS.toArray(new Schema.Field[0]));
  }

  public void testVerifyNoteSchema() {
    Schema expected_schema = getNoteSchema();
    Schema actual_schema = ReadSchemaFromJson.ReadSchema("note.json");
    assertThat(
        actual_schema, sameBeanAs(expected_schema).ignoring("description").ignoring("options"));
  }

  public void testNoteNLPString() {
    Schema expected_schema = getNoteNLPSchema();
    Schema actual_schema = ReadSchemaFromJson.ReadSchema("note_nlp.json");
    assertThat(
        actual_schema, sameBeanAs(expected_schema).ignoring("description").ignoring("options"));
  }

  public void testNoteString() throws IOException {
    String expected_string =
        new String(
            Files.readAllBytes(Paths.get(schema_path + "note.json")), StandardCharsets.UTF_8);
    String actual_string = ReadSchemaFromJson.getJsonString("note.json");
    assertThat(
        actual_string, sameBeanAs(expected_string).ignoring("description").ignoring("options"));
  }

  public void testVerifyNoteNLPSchema() throws IOException {
    String expected_string =
        new String(
            Files.readAllBytes(Paths.get(schema_path + "note_nlp.json")), StandardCharsets.UTF_8);
    String actual_string = ReadSchemaFromJson.getJsonString("note_nlp.json");
    assertThat(
        actual_string, sameBeanAs(expected_string).ignoring("description").ignoring("options"));
  }
}
