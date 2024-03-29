package org.allofus.curation.io.factory;

import org.allofus.curation.io.bigquery.BigQueryRead;
import org.allofus.curation.io.csv.CSVRead;
import org.allofus.curation.io.jsonl.JSONLRead;
import org.allofus.curation.io.parquet.ParquetRead;

public class IOReadFactory {
  public static IORead create(String input_type) {
    if ("csv".equalsIgnoreCase(input_type)) {
      return new CSVRead();
    } else if ("jsonl".equalsIgnoreCase(input_type)) {
      return new JSONLRead();
    } else if ("parquet".equalsIgnoreCase(input_type)) {
      return new ParquetRead();
    } else if ("bigquery".equalsIgnoreCase(input_type)) {
      return new BigQueryRead();
    }
    return null;
  }
}
