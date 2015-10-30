package org.embulk.input.kafka.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.input.kafka.data.format.base.Data;
import org.embulk.input.kafka.data.format.base.Json;
import org.embulk.input.kafka.data.format.ext.MessagePack;
import org.embulk.input.kafka.data.format.ext.example.CustomTsv;
import org.embulk.input.kafka.exception.DataConvertException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

/**
 * convert byte to Record object.
 */
public class DataConverter<T> implements Converter
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final MessagePack magpack = new MessagePack();
    private static final Logger logger = Exec.getLogger(DataConverter.class);
    private static final Data.Builder builder = Data.builder();
    private static final Json.Builder jsonBuilder = Json.builder();

    public static Json convertFromJson(byte[] message)
    {
        try {
            return jsonBuilder.setMessage(message).build();
        } catch (DataConvertException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    @Deprecated
    public static Record convertFromCustomTsv(byte[] message)
    {
        String[] data = new String(message).split("\t");
        CustomTsv tsv = new CustomTsv();
        for (int idx=0; idx<data.length; idx++) {
            tsv.setValueByIndex(idx, data[idx]);
        }
        return tsv;
    }

    public static Data convert(byte[] message, String separator)
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
