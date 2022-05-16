package org.allofus.curation.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by {@link ExampleWordCount}.
 *
 * <p>Inherits standard configuration options.
 */
public interface WordCountOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
}
