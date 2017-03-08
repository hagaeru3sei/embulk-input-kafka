package org.embulk.input.kafka.data;

import java.io.UnsupportedEncodingException;

public class Parse<T> {

  private byte[] in;

  public Parse(T in) {
    this.in = (byte[]) in;
  }

  public Parse<byte[]> apply(T in) {
    return new Parse<byte[]>((byte[]) in);
  }

  public String parseString() throws UnsupportedEncodingException {
    return new String(in, "UTF-8");
  }

  public int parseInt() throws UnsupportedEncodingException {
    return Integer.parseInt(new String(in, "UTF-8"));
  }

  public Long parseLong() throws UnsupportedEncodingException {
    return Long.parseLong(new String(in, "UTF-8"));
  }

  public Double parseDouble() throws UnsupportedEncodingException {
    return Double.parseDouble(new String(in, "UTF-8"));
  }

}
