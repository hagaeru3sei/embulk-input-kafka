package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;

import java.util.ArrayList;
import java.util.List;

public class Data implements Record
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
            set(aData);
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
    public String get(int idx) {
        return record.get(idx);
    }

    @Override
    public int length()
    {
        return record.size();
    }

}
