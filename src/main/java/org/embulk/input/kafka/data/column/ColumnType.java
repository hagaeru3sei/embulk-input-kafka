package org.embulk.input.kafka.data.column;

import org.embulk.input.kafka.exception.ColumnTypeNotFoundException;

import java.util.HashMap;
import java.util.Map;

public enum ColumnType {

  Boolean("boolean"),
  Long("long"),
  Double("double"),
  String("string"),
  Timestamp("timestamp");

  private static final Map<String, ColumnType> types = new HashMap<String, ColumnType>();

  static {
    for (ColumnType type : ColumnType.values()) {
      types.put(type.getName(), type);
    }
  }

  private final String name;

  ColumnType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static ColumnType get(String name) throws ColumnTypeNotFoundException {
    ColumnType type = types.get(name);
    if (type == null) {
      throw new ColumnTypeNotFoundException("Column type not found. type: " + name);
    }
    return type;
  }
}
