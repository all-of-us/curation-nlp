package org.allofus.curation.io.factory;

import org.allofus.curation.io.bigquery.BigQueryWrite;
import org.allofus.curation.io.csv.CSVWrite;
import org.allofus.curation.io.jsonl.JSONLWrite;

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
