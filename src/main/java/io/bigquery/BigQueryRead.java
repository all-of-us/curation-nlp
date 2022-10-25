package io.bigquery;

import io.factory.IORead;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BigQueryRead extends IORead {

  @Override
  public PCollection<Row> expand(PBegin input) {
    return null;
  }
}
