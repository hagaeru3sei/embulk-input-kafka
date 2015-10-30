package org.embulk.input.kafka.data.format.ext.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.input.kafka.data.Record;

/**
 * This is for custom json format sample.
 * If needs a custom logs, please create a new ext classes.
 */
public class CustomJson implements Record<Integer>
{
    private final String version;
    private final String datetime;
    private final String key;
    private final String value;

    @JsonCreator
    private CustomJson(@JsonProperty("version") String version,
                       @JsonProperty("timestamp") String datetime,
                       @JsonProperty("key") String key,
                       @JsonProperty("value") String value)
    {
        this.version = version;
        this.datetime = datetime;
        this.key = key;
        this.value = value;
    }

    public String getVersion()
    {
        return version;
    }

    public String getDatetime()
    {
        return datetime;
    }

    public String getKey()
    {
        return key;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public String get(Integer idx)
    {
        switch (idx)
        {
            case 0: return getVersion();
            case 1: return getDatetime();
            case 2: return getKey();
            case 3: return getValue();
        }
        return "";
    }

    @Override
    public int length() {
        return 4;
    }
}
