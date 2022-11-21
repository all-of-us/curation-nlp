package org.allofus.curation.io.bigquery;

import junit.framework.TestCase;
import org.allofus.curation.io.factory.IORead;
import org.allofus.curation.io.factory.IOReadFactory;
import org.allofus.curation.utils.NLPSchema;
import org.allofus.curation.utils.Constants.Env;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import static org.allofus.curation.utils.Constants.Env.PROJECT_ID;
import static org.allofus.curation.utils.Constants.Env.TEST_DATASET;
import static org.allofus.curation.utils.Constants.Env.TEST_TMP_LOCATION;

public class BigQueryReadTest extends TestCase {
    public void testReadBigQuery() {

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setTempLocation(TEST_TMP_LOCATION);
        options.setRunner(DataflowRunner.class);
        options.setProject(Env.PROJECT_ID);
        options.setRegion("us-east1");
        TestPipeline p = TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
        // TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

        String input_type = "bigquery";
        Schema note_schema = NLPSchema.getNoteSchema();
        String table_name = String.format("%s.%s.%s", PROJECT_ID, TEST_DATASET, "note");

        CoderRegistry cr = p.getCoderRegistry();
        cr.registerCoderForClass(Integer.class, VarIntCoder.of());
        cr.registerCoderForClass(Long.class, VarLongCoder.of());
        cr.registerCoderForClass(Float.class, DoubleCoder.of());

        IORead ioRead = IOReadFactory.create(input_type);
        ioRead.init(table_name, input_type);

        PCollection<org.apache.beam.sdk.values.Row> actual = p.apply(ioRead).setRowSchema(note_schema);

        Row row_1 = Row.withSchema(note_schema)
                .addValue(Long.valueOf("1001"))
                .addValue(Long.valueOf("1"))
                .addValue("2020-10-10")
                .addValue("2020-10-10 01:01:01")
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

        Row row_2 = Row.withSchema(note_schema)
                .addValue(Long.valueOf("3002"))
                .addValue(Long.valueOf("2"))
                .addValue("2020-10-10")
                .addValue("2020-10-10 01:01:01")
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