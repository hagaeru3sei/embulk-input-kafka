package org.embulk.input.kafka.data;

import java.util.List;

public interface Record<T> {

  T get(Object key);

  List<String> keys();

  List<T> values();

  int length();

}
