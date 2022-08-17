package org.allofus.curation.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by {@link ExampleWordCount}.
 *
 * <p>Inherits standard configuration options.
 */
public interface CurationNLPOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);

    @Description("Pipeline from CLAMP")
    @Validation.Required
    String getPipeline();
    void setPipeline(String value);
    
    @Description("UMLS index path")
    @Validation.Required
    String getUmls_index();
    void setUmls_index(String value);
 
    @Description("Build Identifier")
    String getBuildId();
    void setBuildId(String BuildId);
}
