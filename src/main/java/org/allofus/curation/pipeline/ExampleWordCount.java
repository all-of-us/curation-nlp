/*
 * Modified from https://beam.apache.org/get-started/wordcount-example/
 */
package org.allofus.curation.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;

/**
 * Uses Beam WordCount example that counts words in text.
 *
 * <pre>
 *   1. Execute a Pipeline both locally and using the selected runner
 *   2. Use ParDo with static DoFns defined out-of-line
 *   3. Build a composite transform
 *   4. Define pipeline options
 * </pre>
 */
public class ExampleWordCount {

    static void runWordCount(WordCountOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInput()))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        WordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
        runWordCount(options);

        System.exit(0);
    }
}
