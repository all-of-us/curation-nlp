/*
 * Modified from https://beam.apache.org/documentation/pipelines/test-your-pipeline/
*/
package org.allofus.curation.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class CountWordsTest {

    // Our static input data, which will make up the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    public void testCount() {
        // Create a test pipeline.
        Pipeline p = TestPipeline.create();

        // Create an input PCollection.
        PCollection<String> input = p.apply(Create.of(WORDS));

        // Apply the Count transform under test.
        PCollection<KV<String, Long>> output =
                input.apply(Count.<String>perElement());

        // Assert on the results.
        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("hi", 4L),
                        KV.of("there", 1L),
                        KV.of("sue", 2L),
                        KV.of("bob", 2L),
                        KV.of("", 3L),
                        KV.of("ZOW", 1L));

        // Run the pipeline.
        p.run();
    }
}