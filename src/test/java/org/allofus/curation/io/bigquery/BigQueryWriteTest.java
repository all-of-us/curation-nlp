package org.allofus.curation.io.bigquery;

import org.allofus.curation.io.factory.IOWrite;
import org.allofus.curation.io.factory.IOWriteFactory;
import org.allofus.curation.utils.NLPSchema;
import org.allofus.curation.utils.Constants.Env;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;

import junit.framework.TestCase;

import org.apache.beam.sdk.transforms.Create;

public class BigQueryWriteTest extends TestCase {
    public void testWriteBigQuery() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setTempLocation(Env.TEST_TMP_LOCATION);
        options.setStagingLocation(Env.TEST_STAGING_LOCATION);
        options.setRunner(DataflowRunner.class);
        options.setProject(Env.PROJECT_ID);
        options.setSubnetwork(Env.TEST_SUBNETWORK);
        options.setRegion(Env.TEST_REGION);

        TestPipeline p = TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);

        String output_type = "bigquery";
        Schema note_nlp_schema = NLPSchema.getNoteNLPSchema();
        String output_tablename = String.format("%s.%s.%s", Env.PROJECT_ID, Env.TEST_DATASET, "note_nlp");

        CoderRegistry cr = p.getCoderRegistry();
        cr.registerCoderForClass(Integer.class, VarIntCoder.of());
        cr.registerCoderForClass(Long.class, VarLongCoder.of());
        cr.registerCoderForClass(Float.class, DoubleCoder.of());

        IOWrite ioWrite = IOWriteFactory.create(output_type);
        ioWrite.init(output_tablename, output_type);
        // 0,1001,0,test_snippet,0-100,test_variant,100,100,CLAMP
        // 1.7.2,2022-11-10,2022-11-10 10:10:10,False,1 year,TermModifier
        Row row_1 = Row.withSchema(note_nlp_schema)
                .addValue(Long.valueOf("0"))
                .addValue(Long.valueOf("1001"))
                .addValue(Long.valueOf("0"))
                .addValue("test_snippet")
                .addValue("0-100")
                .addValue("test_variant")
                .addValue(Long.valueOf("100"))
                .addValue(Long.valueOf("100"))
                .addValue("CLAMP 1.7.2")
                .addValue("2022-11-10")
                .addValue("2022-11-10 10:10:10")
                .addValue("False")
                .addValue("1 year")
                .addValue("TermModifier")
                .build();

        p.apply(Create.of(row_1)).apply(ioWrite);
        p.run();
        // PCollection<org.apache.beam.sdk.values.Row> actual = p.apply(ioWrite);

    }
}
