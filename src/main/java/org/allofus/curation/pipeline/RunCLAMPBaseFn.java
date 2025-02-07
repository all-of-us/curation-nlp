package org.allofus.curation.pipeline;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;

public abstract class RunCLAMPBaseFn extends PTransform<PCollection<Row>, PCollection<Row>> {

  public void init_clamp(CurationNLPOptions options) throws IOException {
  }
}

