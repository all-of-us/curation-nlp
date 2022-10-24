package org.allofus.curation.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CurationNLPOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("input file extension, e.g: txt, csv, json")
    @Validation.Required
    String getInputExt();
    void setInputExt(String value);

    @Description("output file extension, e.g: csv, note_nlp (table)")
    @Validation.Required
    String getOutputExt();
    void setOutputExt(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);

    @Description("Build Identifier")
    String getBuildId();
    void setBuildId(String BuildId);
}
