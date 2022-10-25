package io.csv;

import io.factory.IOWrite;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.stream.Collectors;

public class CSVWrite extends IOWrite {

  public PDone expand(PCollection<Row> input) {
    return input
        .apply(ParDo.of(new RowToCSV()))
        .apply(
            TextIO.write()
                .to(output_sink)
                .withHeader(
                    output_schema.getFieldNames().stream()
                        .map(Object::toString)
                        .map(StringEscapeUtils::escapeCsv)
                        .collect(Collectors.joining(",")))
                .withSuffix("." + output_ext)
                .withoutSharding());
  }

  public static class RowToCSV extends DoFn<Row, String> {
    @ProcessElement
    public void processElement(@Element Row input, OutputReceiver<String> receiver) {
      String out =
          input.getValues().stream()
              .map(item -> item == null ? "" : item)
              .map(Object::toString)
              .map(StringEscapeUtils::escapeCsv)
              .collect(Collectors.joining(","));
      receiver.output(out);
    }
  }
}
