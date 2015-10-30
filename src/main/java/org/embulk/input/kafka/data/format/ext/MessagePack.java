package org.embulk.input.kafka.data.format.ext;

import org.embulk.input.kafka.data.Record;
import org.msgpack.annotation.Message;

import java.util.List;

// TODO: Change directory.

@Deprecated
@Message
public class MessagePack implements Record<Integer>
{
    private String version;
    private String datetime;
    private String key;
    private String value;

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

    public MessagePack setVersion(String version)
    {
        this.version = version; return this;
    }

    public MessagePack setDatetime(String datetime)
    {
        this.datetime = datetime; return this;
    }

    public MessagePack setKey(String key)
    {
        this.key = key; return this;
    }

    public MessagePack setValue(String value)
    {
        this.value = value; return this;
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
    public List<Integer> getKeys()
    {
        return null;
    }

    @Override
    public int length() {
        return 4;
    }
}
