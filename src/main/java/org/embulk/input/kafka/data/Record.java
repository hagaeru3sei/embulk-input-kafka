package org.embulk.input.kafka.data;

public interface Record
{
    String get(int idx);

    int length();
}
