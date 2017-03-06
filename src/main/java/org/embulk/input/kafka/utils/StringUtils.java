package org.embulk.input.kafka.utils;

public class StringUtils {
  public static String trim(String str, String charactor)
  {
    return str.replaceFirst("^" + charactor, "").replaceFirst(charactor + "$", "");
  }
}
