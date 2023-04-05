package org.allofus.curation.pipeline;

import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import org.allofus.curation.io.factory.IORead;
import org.allofus.curation.io.factory.IOReadFactory;
import org.allofus.curation.io.factory.IOWrite;
import org.allofus.curation.io.factory.IOWriteFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class CurationNLPMain {
  private static final Logger LOG = LoggerFactory.getLogger(CurationNLPMain.class);

  static void runCurationNLP(CurationNLPOptions options)
      throws ConfigurationException, DocumentIOException, IOException {

    Pipeline p = Pipeline.create(options);

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    RunCLAMPFn runCLAMPFn = new RunCLAMPFn();
    runCLAMPFn.init_clamp(options);

    IORead ioRead = IOReadFactory.create(options.getInputType());
    ioRead.init(options.getInput(), options.getInputType());
    IOWrite ioWrite = IOWriteFactory.create(options.getOutputType());
    ioWrite.init(
        options.getOutput(),
        options.getOutputType(),
        options.getMaxOutputBatchSize(),
        options.getMaxOutputPartitionSeconds());

    p.apply(ioRead).apply(runCLAMPFn).apply(ioWrite);

    p.run().waitUntilFinish();
  }

  public static void main(String[] args)
      throws ConfigurationException, DocumentIOException, IOException {
    Instant start = Instant.now();
    CurationNLPOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);

    Instant start2 = Instant.now();
    runCurationNLP(options);
    Instant end2 = Instant.now();
    Duration timeElapsed = Duration.between(start2, end2);
    LOG.info("runCurationNLP: Time taken: " + timeElapsed.toMillis() + " milliseconds");
    Instant end = Instant.now();
    timeElapsed = Duration.between(start, end);
    LOG.info("Time taken: " + timeElapsed.toMillis() + " milliseconds");
    LOG.info("Run CLAMP pipeline done.");

    System.exit(0);
  }
}
