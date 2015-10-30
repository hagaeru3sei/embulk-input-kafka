package org.embulk.input.kafka.data;

import java.util.List;

public interface Record<T>
{
    String get(T key);

    List<T> getKeys();

    int length();
}
