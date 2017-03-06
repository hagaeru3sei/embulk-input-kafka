package org.embulk.input.kafka.data;

public interface Converter {
  public Record convert(byte[] data);
}
