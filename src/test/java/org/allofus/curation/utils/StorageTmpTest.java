package org.allofus.curation.utils;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageTmpTest extends TestCase {

  public void testStoreTmp() throws IOException {
    String resources_dir = "gs://" + Constants.Env.TEST_BUCKET + "/resources";
    String resources_prefix = resources_dir.substring(resources_dir.lastIndexOf("/") + 1);
    String test_filename = "note.jsonl";
    String pipeline_file = resources_prefix + "/pipeline/" + test_filename;
    String umls_dir = resources_prefix + "/index/umls_index";
    String testFolderPath = Constants.ProjectPaths.TEST_INPUT;
    String testFilePath = testFolderPath + "/" + test_filename;
    StorageTmp stmp = new StorageTmp(resources_dir);
    String tmp_file = stmp.StoreTmpFile(pipeline_file);

    HashSet<String> actual_set = new HashSet<String>(FileUtils.readLines(new File(tmp_file)));
    HashSet<String> expected_set = new HashSet<String>(FileUtils.readLines(new File(testFilePath)));

    assertEquals(expected_set, actual_set);

    String tmp_dir = stmp.StoreTmpDir(umls_dir);

    HashSet<String> expected_dir_set;
    try (Stream<Path> paths = Files.walk(Paths.get(testFolderPath))) {
      expected_dir_set =
          paths
              .filter(Files::isRegularFile)
              .map(Path::toString)
              .map(s -> s.substring(s.lastIndexOf("/")))
              .collect(Collectors.toCollection(HashSet::new));
    }
    HashSet<String> actual_dir_set;
    try (Stream<Path> paths = Files.walk(Paths.get(tmp_dir))) {
      actual_dir_set =
          paths
              .filter(Files::isRegularFile)
              .map(Path::toString)
              .map(s -> s.substring(s.lastIndexOf("/")))
              .collect(Collectors.toCollection(HashSet::new));
    }
    assertEquals(expected_dir_set, actual_dir_set);
  }
}
