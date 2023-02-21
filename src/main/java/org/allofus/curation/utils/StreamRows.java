package org.allofus.curation.utils;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class StreamRows extends PTransform<PCollection<Row>, PCollectionList<Row>> {

  @Override
  public PCollectionList<Row> expand(PCollection<Row> input) {
    return PCollectionList.of(input.apply(
        "Windowing",
        Window.<Row>into(FixedWindows.of(Duration.standardMinutes(3)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(1)))
                    .withLateFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(2))))
            .withAllowedLateness(Duration.standardMinutes(10))
            .discardingFiredPanes()));
  }
}
