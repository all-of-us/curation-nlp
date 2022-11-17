package org.allofus.curation.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CurationNLPOptions extends PipelineOptions {

  @Description("Path of the directory/table to read from")
  @Validation.Required
  String getInput();
  void setInput(String value);

  @Description("Path of the directory containing resources for UMLS index, pipeline jar")
  @Validation.Required
  String getResourcesDir();
  void setResourcesDir(String value);

  @Description("Input file type, e.g: csv, jsonl, bigquery")
  @Validation.Required
  String getInputType();
  void setInputType(String value);

  @Description(
    "Pipeline jar to use in resourcesDir/pipeline e.g: clamp-ner.pipeline.jar, clinical_pipeline.pipeline.jar")
  @Validation.Required
  String getPipeline();
  void setPipeline(String value);

  @Description("Output file type, e.g: csv, jsonl, bigquery")
  @Validation.Required
  String getOutputType();
  void setOutputType(String value);

  @Description("Path of the directory/table to write to")
  @Validation.Required
  String getOutput();
  void setOutput(String value);

  @Description("Build Identifier")
  String getBuildId();
  void setBuildId(String BuildId);
}
