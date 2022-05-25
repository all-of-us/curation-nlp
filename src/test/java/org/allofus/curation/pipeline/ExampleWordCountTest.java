/*
 * Modified from https://beam.apache.org/documentation/pipelines/test-your-pipeline/
 */
package org.allofus.curation.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class ExampleWordCountTest {

    // Our static input data, which will comprise the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
            "hi there", "hi", "hi sue bob",
            "hi sue", "", "bob hi"};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    // Our static output data, which is the expected data that the final PCollection must match.
    static final String[] COUNTS_ARRAY = new String[] {
            "hi: 5", "there: 1", "sue: 2", "bob: 2"};

    // Example test that tests the pipeline's transforms.

    public void testCountWords() throws Exception {
        Pipeline p = TestPipeline.create();

        // Create a PCollection from the WORDS static input data.
        PCollection<String> input = p.apply(Create.of(WORDS));

        // Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
        PCollection<String> output = input.apply(new CountWords()).apply(MapElements.via(new FormatAsTextFn()));

        // Assert that the output PCollection matches the COUNTS_ARRAY known static output data.
        PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);

        // Run the pipeline.
        p.run();
    }
}