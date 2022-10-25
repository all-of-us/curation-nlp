package org.allofus.curation.io.jsonl;

import org.allofus.curation.io.factory.IOWrite;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class JSONLWrite extends IOWrite {

  public PDone expand(PCollection<Row> input) {
    return input
        .apply(ToJson.of())
        .apply(TextIO.write().to(output_sink).withSuffix("." + output_ext).withoutSharding());
  }
}
