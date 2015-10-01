package org.embulk.input.kafka;

import org.junit.Test;
import org.msgpack.MessagePack;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestKafkaInputPlugin
{
    // TODO: refactor tests

    @Test
    public void MessagePackTest() throws IOException
    {
        org.embulk.input.kafka.data.format.ext.MessagePack src =
            new org.embulk.input.kafka.data.format.ext.MessagePack();
        src.setVersion("v1")
            .setDatetime("2015-08-23 18:10:00")
            .setKey("key1")
            .setValue("value1");

        MessagePack msgpack = new MessagePack();
        byte[] bytes = msgpack.write(src);
        org.embulk.input.kafka.data.format.ext.MessagePack entity =
            msgpack.read(bytes, org.embulk.input.kafka.data.format.ext.MessagePack.class);

        assertEquals(entity.getVersion(), "v1");
        assertEquals(entity.getDatetime(), "2015-08-23 18:10:00");
        assertEquals(entity.getKey(), "key1");
        assertEquals(entity.getValue(), "value1");
    }

}
