package org.embulk.input.kafka.client.producer.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.embulk.input.kafka.client.Worker;
import org.joda.time.DateTime;
import org.msgpack.MessagePack;

import java.io.IOException;
import java.util.Properties;

public class ExampleProducer extends Worker
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final MessagePack msgpack = new MessagePack();

    @Override
    public void run()
    {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        String topic = "test4";

        org.embulk.input.kafka.data.format.ext.MessagePack record = new org.embulk.input.kafka.data.format.ext.MessagePack();

        for (long i=0; i<10; i++) {
            String version = "v1";
            String datetime = DateTime.now().toString();
            String key = "localhost";
            String value = "test";

            record.setVersion(version)
                .setDatetime(datetime)
                .setKey(key)
                .setValue(value);

            byte[] bytes = new byte[0];
            try {
                bytes = msgpack.write(record);
            } catch (IOException e) {
                e.printStackTrace();
            }

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, new String(bytes));
            producer.send(data);
        }
        producer.close();
    }

    public static void main(String[] args)
    {
        ExampleProducer producer = new ExampleProducer();
        producer.run();
    }
}
