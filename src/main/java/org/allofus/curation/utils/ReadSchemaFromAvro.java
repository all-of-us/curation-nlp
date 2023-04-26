package org.allofus.curation.utils;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.allofus.curation.utils.Constants.ProjectPaths.SCHEMA_CLINICAL_AVRO;

public class ReadSchemaFromAvro {
  private static final Logger LOG = LoggerFactory.getLogger(ReadSchemaFromAvro.class);

  public static org.apache.avro.Schema ReadSchema(String file_name) {
    Schema schema = null;
    String file_path = String.valueOf(Paths.get(SCHEMA_CLINICAL_AVRO + File.separator + file_name));
    try {
      schema = new Schema.Parser().parse(new File(file_path));
    } catch (IOException e) {
      LOG.info(e.getMessage() + ", could not read Avro schema from file " + file_path);
    }
    return schema;
  }
}
