package org.embulk.input.kafka.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.input.kafka.data.format.ext.MessagePack;
import org.embulk.input.kafka.data.format.base.Data;
import org.embulk.input.kafka.data.format.ext.example.Json;
import org.embulk.input.kafka.data.format.ext.example.Tsv;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * convert byte to Record object.
 */
public class DataConverter implements Converter
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final MessagePack magpack = new MessagePack();
    private static final Logger logger = Exec.getLogger(DataConverter.class);
    private static final Data.Builder builder = Data.builder();

    public static Record JsonConverter(byte[] message)
    {
        try {
            return mapper.readValue(new String(message), Json.class);
        } catch (IOException e) {
            logger.error(e.getMessage() + " record: " + new String(message));
        }
        return null;
    }

    @Deprecated
    public static Record TsvConverter(byte[] message)
    {
        String[] data = new String(message).split("\t");
        Tsv tsv = new Tsv();
        for (int idx=0; idx<data.length; idx++) {
            tsv.setValueByIndex(idx, data[idx]);
        }
        return tsv;
    }

    public static Record convert(byte[] message, String separator)
    {
        return builder.setMessage(message).setSeparator(separator).build();
    }

    @Override
    public Record convert(byte[] message)
    {
        // TODO: impl
        return null;
    }
}
