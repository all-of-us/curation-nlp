package org.allofus.curation.pipeline;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
        lineLenDist.update(element.length());
        if (element.trim().isEmpty()) {
            emptyLines.inc();
        }

        String[] words = element.split("[^\\p{L}]+", -1);

        for (String word : words) {
            if (!word.isEmpty()) {
                receiver.output(word);
            }
        }
    }
}