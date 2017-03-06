package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.exception.DataConvertException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class Json implements Record<String> {

  public static class Builder {

    private byte[] message;

    public Builder setMessage(byte[] message) {
      this.message = message; return this;
    }

    public Json build() throws DataConvertException {
      return new Json(this).build();
    }

  }

  private final byte[] message;
  private final Map<String, String> record = new HashMap<String, String>();

  public Json(Builder builder) {
    this.message = builder.message;
  }

  public Json build() throws DataConvertException {
    JSONObject obj;
    try {
      // TODO: follow a order
      // An object is an unordered set of name/value pairs.
      obj = new JSONObject(new String(message));
    } catch (JSONException e) {
      throw new DataConvertException(e.getMessage());
    }
    for (int i=0; i<obj.length(); i++) {
      String key;
      try {
        key = (String)obj.names().get(i);
        record.put(key, obj.getString(key));
      } catch (JSONException e) {
        throw new DataConvertException(e.getMessage());
      }
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
