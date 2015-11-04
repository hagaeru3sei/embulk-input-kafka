package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.exception.DataConvertException;
import org.embulk.input.kafka.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Ltsv implements Record<String>
{
    public static class Builder
    {
        private byte[] message;

        public Builder setMessage(byte[] message)
        {
            this.message = message; return this;
        }

        public Ltsv build() throws DataConvertException
        {
            return new Ltsv(this).build();
        }
    }

    private final byte[] message;
    private final Map<String, String> record = new HashMap<String, String>();

    public Ltsv(Builder builder)
    {
        this.message = builder.message;
    }

    public Ltsv build() throws DataConvertException
    {
        String[] row = new String(message).split("\t");
        for (String col : row) {
            String[] c = col.split(":", 2);
            String key = c[0];
            String value = StringUtils.trim(c[1], "\"");
            record.put(key, value);
        }
        return this;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String get(String key)
    {
        return record.get(key);
    }

    @Override
    public List<String> getKeys() {
        List<String> keys = new ArrayList<String>();
        for (Map.Entry<String, String> entry : record.entrySet()) {
            keys.add(entry.getKey());
        }
        return keys;
    }

    @Override
    public int length()
    {
        return record.size();
    }
}
