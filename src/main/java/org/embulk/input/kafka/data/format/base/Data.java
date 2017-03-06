package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Data implements Record<String> {

  public static class Builder {

    private byte[] message;
    private String separator;
    private String enclosedChar;

    public Builder setMessage(byte[] message) {
      this.message = message; return this;
    }

    public Builder setSeparator(String separator) {
      this.separator = separator; return this;
    }

    public Builder setEnclosedChar(String enclosedChar) {
      this.enclosedChar = enclosedChar; return this;
    }

    public Data build()
    {
      return new Data(this).build();
    }
  }

  private final byte[] message;
  private final String separator;
  private final String enclosedChar;
  private final List<String> record = new ArrayList<String>();

  public Data(Builder builder) {
    this.message = builder.message;
    this.separator = builder.separator;
    this.enclosedChar = builder.enclosedChar;
  }

  public Data build() {
    String[] data = new String(message).split(separator);
    boolean isTrim = !enclosedChar.isEmpty();
    for (String aData : data) {
      if (isTrim) {
        set(StringUtils.trim(aData, enclosedChar));
      } else {
        set(aData);
      }
    }
    return this;
  }

  public void set(int idx, String value) {
    record.add(idx, value);
  }

  public void set(String value) {
    record.add(value);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String get(Object idx) {
    return record.get((Integer) idx);
  }

  @Override
  public List<String> keys() {
    List<String> keys = new ArrayList<String>();
    for (int i=0; i<length(); i++) {
      keys.add(Integer.valueOf(i).toString());
    }
    return keys;
  }

  @Override
  public List<String> values() {
    return record;
  }

  @Override
  public int length() {
    return record.size();
  }

}
