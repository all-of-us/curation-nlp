package org.allofus.curation.utils;

public final class Constants {
  public static class Env {
    public static final String PROJECT_ID = System.getenv("PROJECT_ID");
    public static final String GOOGLE_APPLICATION_CREDENTIALS = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    public static final String TEST_BUCKET = System.getenv("TEST_BUCKET");
    public static final String TEST_DATASET = System.getenv("TEST_DATASET");
    public static final String TEST_TMP_LOCATION = System.getenv("TEST_TMP_LOCATION");
  }

  public static class ProjectPaths {
    public static final String PROJECT_HOME = System.getProperty("user.dir");
    public static final String CLAMP_RESOURCES = PROJECT_HOME + "/resources";
    public static final String RESOURCES = PROJECT_HOME + "/src/main/resources";
    public static final String SCHEMA_CLINICAL = RESOURCES + "/schemas/cdm/clinical";
    public static final String TEST_RESOURCES = PROJECT_HOME + "/src/test/resources";
    public static final String TEST_DATA = TEST_RESOURCES + "/data";
    public static final String TEST_INPUT = TEST_DATA + "/input";
    public static final String TEST_OUTPUT = TEST_DATA + "/output";
    public static final String TEST_EXPECTED = TEST_DATA + "/expected";
  }
}
