package org.allofus.curation.pipeline;

import junit.framework.TestCase;
import org.allofus.curation.io.factory.IORead;
import org.allofus.curation.io.factory.IOReadFactory;
import org.allofus.curation.io.factory.IOWrite;
import org.allofus.curation.io.factory.IOWriteFactory;
import org.allofus.curation.utils.Constants.ProjectPaths;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class PipelineIOTest extends TestCase {

  public void testPipelineIO() throws IOException {
    String ext = "csv";
    String pipeline_jar = "clamp-ner.pipeline.jar";

    String[] args =
        new String[] {
          "--input=" + ProjectPaths.TEST_INPUT + "/",
          "--output=" + ProjectPaths.TEST_OUTPUT + "/",
          "--resourcesDir=" + ProjectPaths.CLAMP_RESOURCES,
          "--pipeline=" + pipeline_jar,
          "--maxClampThreads=4",
          "--maxOutputPartitionSeconds=60",
          "--maxOutputBatchSize=100",
          "--inputType=jsonl",
          "--outputType=" + ext,
        };
    CurationNLPOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);

    Pipeline p = Pipeline.create(options);

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    IORead ioRead = IOReadFactory.create(options.getInputType());
    ioRead.init(options.getInput(), options.getInputType());
    IOWrite ioWrite = IOWriteFactory.create(options.getOutputType());
    ioWrite.init(options.getOutput(), options.getOutputType(),
      options.getMaxOutputBatchSize(),
      options.getMaxOutputPartitionSeconds());

    p.apply(ioRead).apply(new IOFn()).apply(ioWrite);

    p.run().waitUntilFinish();

    File actual = new File(ProjectPaths.TEST_OUTPUT + "/output." + ext);
    File expected = new File(ProjectPaths.TEST_DATA + "/expected/note_nlp." + ext);

    HashSet<String> actual_set = new HashSet<String>(FileUtils.readLines(actual));
    HashSet<String> expected_set = new HashSet<String>(FileUtils.readLines(expected));

    assertEquals(actual_set, expected_set);
  }
}
