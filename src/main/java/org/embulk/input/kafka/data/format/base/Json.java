package org.embulk.input.kafka.data.format.base;

import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.exception.DataConvertException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class Json implements Record<String>
{
    public static class Builder
    {
        private byte[] message;

        public Builder setMessage(byte[] message)
        {
            this.message = message; return this;
        }

        public Json build() throws DataConvertException
        {
            return new Json(this).build();
        }
    }

    private final byte[] message;
    private final Map<String, String> record = new HashMap<String, String>();

    public Json(Builder builder)
    {
        this.message = builder.message;
    }

    public Json build() throws DataConvertException
    {
        JSONObject obj;
        try {
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
    public int length()
    {
        return record.size();
    }
}
