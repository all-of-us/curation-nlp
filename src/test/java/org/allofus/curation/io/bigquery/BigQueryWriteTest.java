package org.allofus.curation.io.bigquery;

import com.google.cloud.bigquery.*;
import junit.framework.TestCase;
import org.allofus.curation.io.factory.IOWrite;
import org.allofus.curation.io.factory.IOWriteFactory;
import org.allofus.curation.utils.NLPSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.allofus.curation.utils.Constants.Env.*;

public class BigQueryWriteTest extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryWriteTest.class);

  BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
  String output_table = PROJECT_ID + "." + TEST_DATASET + "." + TEST_OUTPUT_TABLE;

  @Before
  public void setup() throws InterruptedException {

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder("DELETE FROM `" + output_table + "`" + " WHERE TRUE")
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob.waitFor();
  }

  public void testWriteBigQuery() throws InterruptedException {
    String gcpTempLocation = "gs://" + TEST_BUCKET + "/bq_tmp";
    String[] args = new String[] {"--project=" + PROJECT_ID, "--tempLocation=" + gcpTempLocation};
    TestPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TestPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    String output_type = "bigquery";
    Schema note_nlp_schema = NLPSchema.getNoteNLPSchema();

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    IOWrite ioWrite = IOWriteFactory.create(output_type);
    ioWrite.init(TEST_DATASET + "." + TEST_OUTPUT_TABLE, output_type);

    Row row_1 =
        Row.withSchema(note_nlp_schema)
            .addValue(0L)
            .addValue(1L)
            .addValue(0L)
            .addValue("test_snippet")
            .addValue("0-100")
            .addValue("test_variant")
            .addValue(100L)
            .addValue(100L)
            .addValue("CLAMP 1.7.2")
            .addValue("2022-01-01")
            .addValue("2022-01-01 10:10:10")
            .addValue("False")
            .addValue("1 year")
            .addValue("TermModifier")
            .build();

    Row row_2 =
        Row.withSchema(note_nlp_schema)
            .addValue(0L)
            .addValue(2L)
            .addValue(0L)
            .addValue("test_snippet")
            .addValue("0-100")
            .addValue("test_variant")
            .addValue(100L)
            .addValue(100L)
            .addValue("CLAMP 1.7.2")
            .addValue("2022-01-01")
            .addValue("2022-01-01 10:10:10")
            .addValue("False")
            .addValue("1 year")
            .addValue("TermModifier")
            .build();

    p.apply(Create.of(row_1, row_2)).apply(ioWrite);
    p.run();

    List<String> expected = new ArrayList<>();
    JSONObject json_row_1 = new JSONObject();
    json_row_1.put("note_nlp_id", "0");
    json_row_1.put("note_id", "1");
    json_row_1.put("section_concept_id", "0");
    json_row_1.put("snippet", "test_snippet");
    json_row_1.put("offset", "0-100");
    json_row_1.put("lexical_variant", "test_variant");
    json_row_1.put("note_nlp_concept_id", "100");
    json_row_1.put("note_nlp_source_concept_id", "100");
    json_row_1.put("nlp_system", "CLAMP 1.7.2");
    json_row_1.put("nlp_date", "2022-01-01");
    json_row_1.put("nlp_datetime", "2022-01-01 10:10:10");
    json_row_1.put("term_exists", "False");
    json_row_1.put("term_temporal", "1 year");
    json_row_1.put("term_modifiers", "TermModifier");
    expected.add(json_row_1.toString());

    JSONObject json_row_2 = new JSONObject();
    json_row_2.put("note_nlp_id", "0");
    json_row_2.put("note_id", "2");
    json_row_2.put("section_concept_id", "0");
    json_row_2.put("snippet", "test_snippet");
    json_row_2.put("offset", "0-100");
    json_row_2.put("lexical_variant", "test_variant");
    json_row_2.put("note_nlp_concept_id", "100");
    json_row_2.put("note_nlp_source_concept_id", "100");
    json_row_2.put("nlp_system", "CLAMP 1.7.2");
    json_row_2.put("nlp_date", "2022-01-01");
    json_row_2.put("nlp_datetime", "2022-01-01 10:10:10");
    json_row_2.put("term_exists", "False");
    json_row_2.put("term_temporal", "1 year");
    json_row_2.put("term_modifiers", "TermModifier");
    expected.add(json_row_2.toString());

    // Validate
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder("SELECT * FROM `" + output_table + "`" + " WHERE TRUE")
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    TableResult result = queryJob.getQueryResults();

    List<String> actual = new ArrayList<>();
    for (FieldValueList tableRow : result.iterateAll()) {
      JSONObject row = new JSONObject();
      for (Schema.Field field : note_nlp_schema.getFields()) {
        if (Objects.equals(field.getName(), "nlp_datetime")) {
          long epoch = Double.valueOf(tableRow.get(field.getName()).getStringValue()).longValue();
          ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epoch), ZoneOffset.UTC);
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
          row.put(field.getName(), zonedDateTime.format(formatter));
        } else {
          row.put(field.getName(), tableRow.get(field.getName()).getStringValue());
        }
      }
      actual.add(row.toString());
    }

    assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
  }

  @After
  public void tearDown() throws InterruptedException {

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder("DELETE FROM `" + output_table + "`" + " WHERE TRUE")
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob.waitFor();
  }
}
