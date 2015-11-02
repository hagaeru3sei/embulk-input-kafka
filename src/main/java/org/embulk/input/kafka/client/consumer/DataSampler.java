package org.embulk.input.kafka.client.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.embulk.input.kafka.data.DataConverter;
import org.embulk.input.kafka.data.DataType;
import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.exception.DataTypeNotFoundException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class DataSampler implements Runnable, Sampler
{

    private static final int SAMPLING_COUNT = 2;

    private volatile List<List<String>> sampled;

    private KafkaStream stream;
    private Logger logger = Exec.getLogger(ConsumerWorker.class);
    private DataType format;

    public DataSampler(KafkaStream stream, DataType format, List<List<String>> sampled)
    {
        this.stream = stream;
        this.format = format;
        this.sampled = sampled;
    }


    @Override
    public void run()
    {
        sampling();
    }

    private Record getRecord(byte[] message) throws DataTypeNotFoundException
    {
        Record record = null;

        switch (format)
        {
            case Csv: record = DataConverter.convert(message, ","); break;
            case Tsv: record = DataConverter.convert(message, "\t"); break;
            case Ltsv: record = DataConverter.convertFromLtsv(message); break;
            case Json: record = DataConverter.convertFromJson(message); break;
            case MessagePack:
                // TODO: implement
                // NOTE: message pack is not compiled template by this thread.
                break;
        }
        return record;
    }

    @Override
    public void sampling() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        int counter = 0;
        while (it.hasNext()) {
            Record record = null;
            try {
                record = getRecord(it.next().message());
            } catch (DataTypeNotFoundException e) {
                logger.error(e.getMessage());
            }

            if (record == null) {
                logger.warn("record is null.");
                continue;
            }

            List<String> r = new ArrayList<String>();
            for (int idx=0; idx<record.length(); idx++) {
                switch (format) {
                    case Json: r.add(record.get(record.getKeys().get(idx))); break;
                    default: r.add(record.get(idx));
                }
            }
            sampled.add(r);
            counter++;
            if (counter >= SAMPLING_COUNT) break;
        }
    }
}
