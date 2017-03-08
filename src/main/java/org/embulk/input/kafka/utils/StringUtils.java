package org.embulk.input.kafka.utils;

public class StringUtils {
  public static String trim(String str, String character) {
    return str.replaceFirst("^" + character, "").replaceFirst(character + "$", "");
  }
}
