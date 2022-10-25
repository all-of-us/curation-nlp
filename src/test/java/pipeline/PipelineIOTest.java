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

public class PipelineIOTest extends TestCase {

  private String args;
  CurationNLPOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);

  public void testPipelineIO() {
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
  }
}
