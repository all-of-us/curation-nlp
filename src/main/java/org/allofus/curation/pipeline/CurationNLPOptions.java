package org.allofus.curation.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CurationNLPOptions extends PipelineOptions {

    @Description("Path of the directory to read from")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Path of the directory containing resources for UMLS index, pipeline jar")
    @Validation.Required
    String getResourcesDir();
    void setResourcesDir(String value);

    @Description("input file extension, e.g: csv, jsonl, bigquery")
    @Validation.Required
    String getInputExt();
    void setInputExt(String value);

    @Description("output file extension, e.g: csv, jsonl, bigquery")
    @Validation.Required
    String getOutputExt();
    void setOutputExt(String value);

    @Description("Path of the directory to write to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);

    @Description("Build Identifier")
    String getBuildId();
    void setBuildId(String BuildId);
}
