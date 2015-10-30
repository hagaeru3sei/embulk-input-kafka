package org.embulk.input.kafka.data;

public interface Record<T>
{
    String get(T key);

    int length();
}
