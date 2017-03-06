package org.embulk.input.kafka.data.format.ext.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.input.kafka.data.Record;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is for custom json format sample.
 * If needs a custom logs, please create a new ext classes.
 */
public class CustomJson implements Record<String> {

  private final String version;
  private final String datetime;
  private final String key;
  private final String value;
  private final Map<String, String> record = new LinkedHashMap<String, String>();

  @JsonCreator
  private CustomJson(@JsonProperty("version") String version,
             @JsonProperty("timestamp") String datetime,
             @JsonProperty("key") String key,
             @JsonProperty("value") String value) {
    this.version = version;
    this.datetime = datetime;
    this.key = key;
    this.value = value;
  }

  public String getVersion() {
    return version;
  }

  public String getDatetime() {
    return datetime;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String get(Object idx) {
    switch (idx.toString()) {
      case "0": return getVersion();
      case "1": return getDatetime();
      case "2": return getKey();
      case "3": return getValue();
    }
    return "";
  }

  @Override
  public List<String> keys() {
    // TODO: implements
    return null;
  }

  @Override
  public List<String> values() {
    // TODO: implements
    return null;
  }

  @Override
  public int length() {
    return 4;
  }
}
