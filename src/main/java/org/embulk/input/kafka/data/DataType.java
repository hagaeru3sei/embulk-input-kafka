package org.embulk.input.kafka.data;

import org.embulk.input.kafka.exception.DataTypeNotFoundException;

import java.util.HashMap;
import java.util.Map;

public enum DataType
{
    Json("json"),
    MessagePack("msgpack"),
    Ltsv("ltsv"),
    Tsv("tsv"),
    Csv("csv");

    private static final Map<String, DataType> types = new HashMap<String, DataType>();

    static {
        for (DataType type : DataType.values()) {
            types.put(type.getName(), type);
        }
    }

    private final String name;

    DataType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static DataType get(String name) throws DataTypeNotFoundException {
        DataType type = types.get(name);
        if (type == null) {
            throw new DataTypeNotFoundException("Data type not found. type: " + name);
        }
        return type;
    }
}
