package pipeline;

import io.factory.IORead;
import io.factory.IOReadFactory;
import io.factory.IOWrite;
import io.factory.IOWriteFactory;
import junit.framework.TestCase;
import org.allofus.curation.pipeline.CurationNLPOptions;
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

  String ext = "csv";
  String home_dir = System.getProperty("user.dir");
  String data_path = home_dir + "/src/test/resources/data";
  String resources_path = home_dir + "resources";

  String[] args =
      new String[] {
        "--input=" + data_path + "/input/",
        "--output=" + data_path + "/output/",
        "--resourcesDir=" + resources_path,
        "--inputType=jsonl",
        "--outputType=" + ext,
      };
  CurationNLPOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);

  public void testPipelineIO() throws IOException {

    Pipeline p = Pipeline.create(options);

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    IORead ioRead = IOReadFactory.create(options.getInputType());
    ioRead.init(options.getInput(), options.getInputType());
    IOWrite ioWrite = IOWriteFactory.create(options.getOutputType());
    ioWrite.init(options.getOutput(), options.getOutputType());

    p.apply(ioRead).apply(new IOFn()).apply(ioWrite);

    p.run().waitUntilFinish();

    File actual = new File(data_path + "/output/output." + ext);
    File expected = new File(data_path + "/expected/note_nlp." + ext);

    HashSet<String> actual_set = new HashSet<String>(FileUtils.readLines(actual));
    HashSet<String> expected_set = new HashSet<String>(FileUtils.readLines(expected));

    assertTrue(actual_set.equals(expected_set));
  }
}
