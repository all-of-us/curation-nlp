package org.allofus.curation.utils;

public class SanitizeInput {

  public static String sanitize(String input){
    return input.endsWith("/")
      ? input.substring(0, input.length() - 1)
      : input;
  }
}
