package org.embulk.input.kafka.data.format.ext.example;

import org.embulk.input.kafka.data.Record;

/**
 * This is for custom tsv format sample.
 * If needs a custom logs, please create a new ext classes.
 */
public class CustomTsv implements Record<Integer> {

    private String version;
    private String datetime;
    private String key;
    private String value;

    public CustomTsv setVersion(String version)
    {
        this.version = version; return this;
    }

    public void setDatetime(String datetime)
    {
        this.datetime = datetime;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public void setValueByIndex(int idx, String s)
    {
        switch (idx)
        {
            case 0: setVersion(s); break;
            case 1: setDatetime(s); break;
            case 2: setKey(s); break;
            case 3: setValue(s); break;
        }
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
