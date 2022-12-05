package org.allofus.curation.io.bigquery;

import junit.framework.TestCase;
import org.allofus.curation.io.factory.IORead;
import org.allofus.curation.io.factory.IOReadFactory;
import org.allofus.curation.utils.NLPSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import static org.allofus.curation.utils.Constants.Env.*;

public class BigQueryReadTest extends TestCase {

  public void testReadBQ() {
    String gcpTempLocation = "gs://" + TEST_BUCKET + "/bq_tmp";
    String[] args = new String[] {"--project=" + PROJECT_ID, "--tempLocation=" + gcpTempLocation};
    TestPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TestPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    String input_type = "bigquery";
    Schema note_schema = NLPSchema.getNoteSchema();

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    IORead ioRead = IOReadFactory.create(input_type);
    ioRead.init(PROJECT_ID + "." + TEST_DATASET + "." + TEST_INPUT_TABLE, input_type);

    PCollection<Row> actual = p.apply(ioRead).setRowSchema(note_schema);

    Row row_1 =
        Row.withSchema(note_schema)
            .addValue(Long.valueOf("1001"))
            .addValue(Long.valueOf("1"))
            .addValue("2020-10-10")
            .addValue("2020-10-10 01:01:01 UTC")
            .addValue(Long.valueOf("3522232"))
            .addValue(Long.valueOf("0"))
            .addValue("Administrative note")
            .addValue(
                "This note contains a few characters like headache.This note contains a few characters like headache. This note contains a few characters like headache")
            .addValue(Long.valueOf("1147732"))
            .addValue(Long.valueOf("4180186"))
            .addValue(Long.valueOf("1234"))
            .addValue(Long.valueOf("123456"))
            .addValue(Long.valueOf("1234567"))
            .addValue("5")
            .build();

    Row row_2 =
        Row.withSchema(note_schema)
            .addValue(Long.valueOf("3002"))
            .addValue(Long.valueOf("2"))
            .addValue("2020-10-10")
            .addValue("2020-10-10 01:01:01 UTC")
            .addValue(Long.valueOf("3522232"))
            .addValue(Long.valueOf("0"))
            .addValue("Administrative note")
            .addValue(
                "This is a different note that contains a few characters like acne.This is a different note that contains a few characters like acne. This is a different note that contains a few characters like acne")
            .addValue(Long.valueOf("1147732"))
            .addValue(Long.valueOf("4180186"))
            .addValue(Long.valueOf("1234"))
            .addValue(Long.valueOf("23456"))
            .addValue(Long.valueOf("234567"))
            .addValue("6")
            .build();

    PAssert.that(actual).containsInAnyOrder(row_1, row_2);

    p.run();
  }
}
