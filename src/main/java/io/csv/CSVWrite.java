package io.csv;

import io.factory.IOWrite;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class CSVWrite extends IOWrite {

  public PDone expand(PCollection<Row> input) {
    return input
        .apply(ToString.elements())
        .apply(TextIO.write().to(output_sink).withSuffix("." + output_ext));
  }
}
