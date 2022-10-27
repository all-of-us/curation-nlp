package org.allofus.curation.utils;

import junit.framework.TestCase;

public class SanitizeInputTest extends TestCase {

  public void testSanitizeBadInput() {
    String input = "Folder/";
    String expected = "Folder";
    String actual = SanitizeInput.sanitize(input);
    assertEquals(expected, actual);
  }

  public void testSanitizeGoodInput() {
    String input = "Folder";
    String expected = "Folder";
    String actual = SanitizeInput.sanitize(input);
    assertEquals(expected, actual);
  }
}
