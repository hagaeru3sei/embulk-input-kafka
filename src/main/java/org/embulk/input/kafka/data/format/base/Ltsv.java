package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.exception.DataConvertException;
import org.embulk.input.kafka.utils.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Ltsv implements Record<String> {

  public static class Builder {

    private byte[] message;
    private String enclosedChar;

    public Builder setMessage(byte[] message) {
      this.message = message; return this;
    }

    public Builder setEnclosedChar(String enclosedChar) {
       this.enclosedChar = enclosedChar; return this;
    }

    public Ltsv build() throws DataConvertException {
      return new Ltsv(this).build();
    }

  }

  private final byte[] message;
  private final String enclosedChar;
  private final Map<String, String> record = new LinkedHashMap<String, String>();

  public Ltsv(Builder builder) {
    this.message = builder.message;
    this.enclosedChar = builder.enclosedChar;
  }

  public Ltsv build() throws DataConvertException {
    String[] row = new String(message).split("\t");
    boolean isTrim = !enclosedChar.isEmpty();
    for (String col : row) {
      String[] c = col.split(":", 2);
      String key, value;
      if (isTrim) {
        key = StringUtils.trim(c[0].trim(), enclosedChar);
        value = StringUtils.trim(c[1].trim(), enclosedChar);
      } else {
        key = c[0];
        value = c[1];
      }
      record.put(key, value);
    }
    return this;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String get(Object key) {
    return record.get(key);
  }

  @Override
  public List<String> keys() {
    List<String> keys = new ArrayList<String>();
    for (Map.Entry<String, String> entry : record.entrySet()) {
      keys.add(entry.getKey());
    }
    return keys;
  }

  @Override
  public List<String> values() {
    List<String> values = new ArrayList<String>();
    for (Map.Entry<String, String> entry : record.entrySet()) {
      values.add(entry.getValue());
    }
    return values;
  }

  @Override
  public int length() {
    return record.size();
  }
}
