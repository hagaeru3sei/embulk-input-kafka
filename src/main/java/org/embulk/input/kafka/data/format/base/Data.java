package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Data implements Record<Integer>
{

    public static class Builder
    {
        private byte[] message;
        private String separator;

        public Builder setMessage(byte[] message)
        {
            this.message = message; return this;
        }

        public Builder setSeparator(String separator)
        {
            this.separator = separator; return this;
        }

        public Data build()
        {
            return new Data(this).build();
        }
    }

    private final byte[] message;
    private final String separator;
    private final List<String> record = new ArrayList<String>();

    public Data(Builder builder)
    {
        this.message = builder.message;
        this.separator = builder.separator;
    }

    public Data build()
    {
        String[] data = new String(message).split(separator);
        for (String aData : data) {
            set(StringUtils.trim(aData, "\""));
        }
        return this;
    }

    public void set(int idx, String value)
    {
        record.add(idx, value);
    }

    public void set(String value)
    {
        record.add(value);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String get(Integer idx) {
        return record.get(idx);
    }

    @Override
    public List<Integer> getKeys() {
        List<Integer> keys = new ArrayList<Integer>();
        for (int i=0; i<length(); i++) {
            keys.add(i);
        }
        return keys;
    }

    @Override
    public int length()
    {
        return record.size();
    }

}
