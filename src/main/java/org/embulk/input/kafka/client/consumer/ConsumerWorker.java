package org.embulk.input.kafka.client.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.embulk.input.kafka.data.DataConverter;
import org.embulk.input.kafka.data.DataType;
import org.embulk.input.kafka.data.Record;
import org.embulk.input.kafka.data.column.ColumnType;
import org.embulk.input.kafka.exception.ColumnTypeNotFoundException;
import org.embulk.input.kafka.exception.DataTypeNotFoundException;
import org.embulk.input.kafka.exception.DateFormatException;
import org.embulk.input.kafka.utils.DateUtils;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerWorker implements Runnable
{
    private final KafkaStream stream;
    private final int threadNumber;
    private final SchemaConfig columns;
    private volatile AtomicInteger counter;
    private final Logger logger = Exec.getLogger(ConsumerWorker.class);
    private final PageBuilder pageBuilder;
    private final DataType format;
    private final int ignoreLines;
    private final int previewSamplingCount;
    private final String enclosedChar;
    private SimpleDateFormat dateFormat;

    public ConsumerWorker(
        KafkaStream stream,
        int threadNumber,
        SchemaConfig columns,
        AtomicInteger counter,
        PageBuilder pageBuilder,
        DataType format,
        int ignoreLines,
        int previewSamplingCount,
        String enclosedChar) throws DataTypeNotFoundException {

        this.threadNumber = threadNumber;
        this.stream = stream;
        this.columns = columns;
        this.counter = counter;
        this.pageBuilder = pageBuilder;
        this.format = format;
        this.ignoreLines = ignoreLines;
        this.previewSamplingCount = previewSamplingCount;
        this.enclosedChar = enclosedChar;
    }

    @Override
    public void run()
    {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        logger.info("Starting Thread: " + threadNumber);

        int loopCounter = 0;
        while (it.hasNext()) {
            Record record = null;
            try {
                record = getRecord(it.next().message());
            } catch (DataTypeNotFoundException e) {
                logger.error(e.getMessage());
            }

            //// skip ignore header lines
            if (++loopCounter <= ignoreLines) {
                logger.info("Skip header lines. line: " + loopCounter);
                continue;
            }

            if (Exec.isPreview() && loopCounter - ignoreLines > previewSamplingCount) {
                logger.info("Skip lines.");
                break;
            }

            if (record == null) {
                logger.warn("record is null.");
                continue;
            }

            synchronized (pageBuilder) {
                synchronized (columns) {
                    int idx = 0;
                    for (ColumnConfig column : columns.getColumns()) {
                        Column col = column.toColumn(idx);
                        try {
                            switch (format) {
                                case Json:
                                case Ltsv:
                                    setColumn(col, record.get(col.getName()));
                                    break;
                                default:
                                    setColumn(col, record.get(idx));
                                    break;
                            }
                        } catch (ColumnTypeNotFoundException | DateFormatException | ParseException e) {
                            logger.error(e.getMessage());
                        }

                        idx++;
                    }
                }
                pageBuilder.addRecord();
            }
            counter.incrementAndGet();
        }
        logger.info("Shutting down Thread: " + threadNumber);
    }

    private Record getRecord(byte[] message) throws DataTypeNotFoundException
    {
        Record record = null;
        switch (format)
        {
            case Csv: record = DataConverter.convert(message, ",", enclosedChar); break;
            case Tsv: record = DataConverter.convert(message, "\t", enclosedChar); break;
            case Ltsv: record = DataConverter.convertFromLtsv(message, enclosedChar); break;
            case Json: record = DataConverter.convertFromJson(message); break;
            case MessagePack:
                // TODO: implement
                // NOTE: message pack is not compiled template by this thread.
                break;
        }
        return record;
    }

    private synchronized void setColumn(Column column, String value)
        throws ColumnTypeNotFoundException, DateFormatException, ParseException
    {
        switch (ColumnType.get(column.getType().getName()))
        {
            case Boolean:  pageBuilder.setBoolean(column, Boolean.valueOf(value)); break;
            case Long:     pageBuilder.setLong(column, Long.parseLong(value)); break;
            case Double:   pageBuilder.setDouble(column, Double.parseDouble(value)); break;
            case String:   pageBuilder.setString(column, value); break;
            case Timestamp:
                if (dateFormat == null) dateFormat = DateUtils.format(value);
                pageBuilder.setTimestamp(
                    column,
                    Timestamp.ofEpochSecond(dateFormat.parse(value).getTime()/1000));
                break;
        }
    }

}
