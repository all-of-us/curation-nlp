package io.factory;

import io.bigquery.BigQueryWrite;
import io.csv.CSVWrite;
import io.jsonl.JSONLWrite;

public class IOWriteFactory {
  public static IOWrite create(String output_type) {
    if ("csv".equalsIgnoreCase(output_type)) {
      return new CSVWrite();
    } else if ("jsonl".equalsIgnoreCase(output_type)) {
      return new JSONLWrite();
    } else if ("bigquery".equalsIgnoreCase(output_type)) {
      return new BigQueryWrite();
    }
    return null;
  }
}
