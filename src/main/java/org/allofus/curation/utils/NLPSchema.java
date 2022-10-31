package org.allofus.curation.utils;

import org.apache.beam.sdk.schemas.Schema;

import java.util.LinkedList;
import java.util.List;

public class NLPSchema {
  public static Schema getNoteSchema() {
    List<org.apache.beam.sdk.schemas.Schema.Field> FIELDS = new LinkedList<>();
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("person_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_date", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_datetime", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_type_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_class_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_title", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_text", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("encoding_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("language_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("provider_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("visit_occurrence_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("visit_detail_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_source_value", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    return org.apache.beam.sdk.schemas.Schema.of(FIELDS.toArray(new org.apache.beam.sdk.schemas.Schema.Field[0]));
  }
  public static Schema getNoteNLPSchema() {
    List<org.apache.beam.sdk.schemas.Schema.Field> FIELDS = new LinkedList<>();
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_nlp_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("section_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("snippet", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("offset", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("lexical_variant", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_nlp_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("note_nlp_source_concept_id", org.apache.beam.sdk.schemas.Schema.FieldType.INT64));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("nlp_system", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("nlp_date", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("nlp_datetime", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("term_exists", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("term_temporal", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    FIELDS.add(org.apache.beam.sdk.schemas.Schema.Field.nullable("term_modifiers", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    return org.apache.beam.sdk.schemas.Schema.of(FIELDS.toArray(new org.apache.beam.sdk.schemas.Schema.Field[0]));
  }
}
