package org.allofus.curation.pipeline;

public class RunCLAMPFnFactory {
  public static RunCLAMPBaseFn create(CurationNLPOptions options) {
    if (options.getPipeline().contains("deid")) {
      return new RunCLAMPDeidFn();
    } else {
      return new RunCLAMPNLPFn();
    }
  }
}