package io.factory;

import io.bigquery.BigQueryRead;
import io.csv.CSVRead;
import io.jsonl.JSONLRead;

public class IOReadFactory {
  public static IORead create(String input_type) {
    if ("csv".equalsIgnoreCase(input_type)) {
      return new CSVRead();
    } else if ("jsonl".equalsIgnoreCase(input_type)) {
      return new JSONLRead();
    } else if ("bigquery".equalsIgnoreCase(input_type)) {
      return new BigQueryRead();
    }
    return null;
  }
}
