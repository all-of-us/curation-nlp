package org.allofus.curation.io.jsonl;

import org.allofus.curation.io.factory.IOWrite;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class JSONLWrite extends IOWrite {

  Integer maxOutputPartitionSeconds = 60;
  Integer outputBatchSize = 1000;

  public PDone expand(PCollection<Row> input) {
    return input
        .apply(ToJson.of())
        .apply("Windowing",
          Window.<String>into(FixedWindows.of(Duration.standardSeconds(maxOutputPartitionSeconds)))
            .triggering(Repeatedly.forever(
              AfterFirst.of(AfterPane.elementCountAtLeast(outputBatchSize),
                AfterProcessingTime
                  .pastFirstElementInPane()
                  .plusDelayOf(Duration.standardSeconds(maxOutputPartitionSeconds/4)))))
            .withAllowedLateness(Duration.standardSeconds(maxOutputPartitionSeconds/12))
            .discardingFiredPanes())
        .apply(TextIO.write().to(output_sink).withWindowedWrites().withNumShards(1).withSuffix("." + output_ext));
  }
}
