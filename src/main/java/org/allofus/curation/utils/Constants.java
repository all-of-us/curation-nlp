package org.allofus.curation.utils;

import java.io.File;

public final class Constants {
  public static class Env {
    public static final String PROJECT_ID = System.getenv("PROJECT_ID");
    public static final String TEST_BUCKET = System.getenv("TEST_BUCKET");
    public static final String TEST_DATASET = System.getenv("TEST_DATASET");
    public static final String TEST_INPUT_TABLE = System.getenv("TEST_INPUT_TABLE");
    public static final String TEST_OUTPUT_TABLE = System.getenv("TEST_OUTPUT_TABLE");
  }

  public static class ProjectPaths {
    public static final String PROJECT_HOME = System.getProperty("user.dir");
    public static final String CLAMP_RESOURCES = PROJECT_HOME + File.separator + "resources";
    public static final String RESOURCES =
        PROJECT_HOME
            + File.separator
            + "src"
            + File.separator
            + "main"
            + File.separator
            + "resources";
    public static final String SCHEMA_CLINICAL =
        RESOURCES
            + File.separator
            + "schemas"
            + File.separator
            + "cdm"
            + File.separator
            + "clinical";
    public static final String SCHEMA_CLINICAL_JSON = SCHEMA_CLINICAL + File.separator + "json";
    public static final String SCHEMA_CLINICAL_AVRO = SCHEMA_CLINICAL + File.separator + "avro";
    public static final String TEST_RESOURCES =
        PROJECT_HOME
            + File.separator
            + "src"
            + File.separator
            + "test"
            + File.separator
            + "resources";
    public static final String TEST_DATA = TEST_RESOURCES + File.separator + "data";
    public static final String TEST_INPUT = TEST_DATA + File.separator + "input";
    public static final String TEST_OUTPUT = TEST_DATA + File.separator + "output";
    public static final String TEST_EXPECTED = TEST_DATA + File.separator + "expected";
  }
}
