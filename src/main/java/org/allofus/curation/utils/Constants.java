package org.allofus.curation.utils;

public class Constants {
  public static class Env {
    public static final String PROJECT_ID = System.getProperty("PROJECT_ID");
    public static final String GOOGLE_APPLICATION_CREDENTIALS =
        System.getProperty("GOOGLE_APPLICATION_CREDENTIALS");
    public static final String TEST_BUCKET = System.getProperty("TEST_BUCKET");
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
