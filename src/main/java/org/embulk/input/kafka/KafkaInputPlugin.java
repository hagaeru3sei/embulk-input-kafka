package org.embulk.input.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.embulk.config.*;
import org.embulk.exec.NoSampleException;
import org.embulk.input.kafka.client.consumer.ConsumerWorker;
import org.embulk.input.kafka.client.consumer.DataSampler;
import org.embulk.input.kafka.data.DataType;
import org.embulk.input.kafka.exception.DataTypeNotFoundException;
import org.embulk.input.kafka.utils.StringUtils;
import org.embulk.spi.*;
import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaInputPlugin implements InputPlugin
{

    // TODO: move to config
    private final static String DELIMITER_DOMAIN_PORT = ":";

    private Logger logger = Exec.getLogger(KafkaInputPlugin.class);

    public interface PluginTask extends Task
    {
        @Config("host")
        String getHost();

        @Config("port")
        String getPort();

        @Config("topic")
        String getTopic();

        @Config("group.id")
        String getGroupId();

        @Config("zookeeper.session.timeout.ms")
        String getZookeeperSessionTimeoutMs();

        @Config("zookeeper.sync.time.ms")
        String getZookeeperSyncTimeMs();

        @Config("auto.commit.interval.ms")
        String getAutoCommitIntervalMs();

        @Config("auto.offset.reset")
        String getAutoOffsetReset();

        @Config("ignore.lines")
        @ConfigDefault("0")
        String getIgnoreLines();

        @Config("data.format")
        @ConfigDefault("null")
        String getDataFormat();

        @Config("data.columns")
        SchemaConfig getColumns();

        @Config("preview.sampling.count")
        @ConfigDefault("10")
        int getPreviewSamplingCount();

        @Config("data.column.enclosedChar")
        @ConfigDefault("")
        String getEnclosedChar();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    public interface GuessTask extends Task
    {
        @Config("host")
        String getHost();

        @Config("port")
        String getPort();

        @Config("topic")
        String getTopic();

        @Config("zookeeper.session.timeout.ms")
        String getZookeeperSessionTimeoutMs();

        @Config("zookeeper.sync.time.ms")
        String getZookeeperSyncTimeMs();

        @Config("auto.offset.reset")
        String getAutoOffsetReset();

        @Config("ignore.lines")
        @ConfigDefault("0")
        String getIgnoreLines();

        @Config("data.format")
        String getDataFormat();

        @Config("data.column.enclosedChar")
        @ConfigDefault("")
        String getEnclosedChar();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(
        TaskSource taskSource,
        Schema schema,
        int taskCount,
        InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(
        TaskSource taskSource,
        Schema schema,
        int i,
        List<TaskReport> list)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
                          Schema schema, int taskIndex,
                          PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        SchemaConfig columns = task.getColumns();
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        ConsumerConfig config = getConsumerConfig(task);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        List<KafkaStream<byte[], byte[]>> streams = getStreams(task, consumer);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        AtomicInteger counter = new AtomicInteger(0);

        int threadNumber = 0;
        DataType format = null;
        try {
            format = DataType.get(task.getDataFormat());
        } catch (DataTypeNotFoundException e) {
            logger.error(e.getMessage());
        }
        for (KafkaStream stream : streams) {
            try {
                executor.submit(
                    new ConsumerWorker(
                        stream,
                        threadNumber,
                        columns,
                        counter,
                        pageBuilder,
                        format,
                        Integer.valueOf(task.getIgnoreLines()),
                        task.getPreviewSamplingCount(),
                        task.getEnclosedChar()));
            } catch (DataTypeNotFoundException e) {
                logger.error(e.getMessage());
            }
            threadNumber++;
        }

        int savedCount;
        while (!executor.isShutdown()) {
            savedCount = counter.get();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }

            if (savedCount < counter.get()) continue;

            consumer.commitOffsets();

            shutdown(consumer, executor);
            pageBuilder.finish();

            logger.info("exec count: " + counter);
        }

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        // TODO: refactor guess
        GuessTask task = config.loadConfig(GuessTask.class);

        Properties props = new Properties();
        props.put("zookeeper.connect", task.getHost() + ":" + task.getPort());
        props.put("group.id", "guess-" + UUID.randomUUID().toString()); // --from-beginning
        props.put("zookeeper.session.timeout.ms", task.getZookeeperSessionTimeoutMs());
        props.put("zookeeper.sync.time.ms", task.getZookeeperSyncTimeMs());
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", task.getAutoOffsetReset());

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        HashMap<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(task.getTopic(), 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(task.getTopic());

        ExecutorService executor = Executors.newSingleThreadExecutor();

        List<List<String>> sampled = new ArrayList<List<String>>();
        DataType dataType = null;
        try {
            dataType = DataType.get(task.getDataFormat());
        } catch (DataTypeNotFoundException e) {
            logger.error(e.getMessage());
        }
        for (KafkaStream stream : streams) {
            executor.submit(new DataSampler(stream, dataType, sampled, task.getEnclosedChar()));
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        if (sampled.size() == 0) {
            throw new NoSampleException("Can't get sample data because the first input line is empty");
        }

        // Guess a column name from header line.
        List<Map<String, String>> columns = new ArrayList<Map<String, String>>();
        List<String> header = sampled.get(0);
        int idx = 0;
        for (String th : header) {
            Map<String, String> column = new HashMap<String, String>();
            String name;
            if (task.getIgnoreLines().equals("0")) {
                name = "c" + String.valueOf(idx);
            } else {
                name = th;
            }
            column.put("name", name);
            column.put("type", "");
            columns.add(idx, column);
            idx++;
        }

        // Guess a column type from values.
        // TODO: increase sampling count
        List<String> sample = sampled.get(1);
        idx = 0;
        for (String value : sample) {
            System.out.println(value);
            if (!task.getEnclosedChar().isEmpty()) {
                value = StringUtils.trim(value, task.getEnclosedChar());
            }
            if (value.equals("true") || value.equals("false")) {
                columns.get(idx).put("type", "boolean");
                idx++;
                continue;
            }
            try {
                Integer.parseInt(value);
                columns.get(idx).put("type", "long");
                idx++;
                continue;
            } catch (NumberFormatException e) {
                logger.debug(e.getMessage());
            }
            try {
                Double.parseDouble(value);
                columns.get(idx).put("type", "double");
                idx++;
                continue;
            } catch (NumberFormatException e) {
                logger.debug(e.getMessage());
            }
            try {
                // TODO: guess format and Add date util class
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                format.parse(value);
                columns.get(idx).put("type", "timestamp");
                columns.get(idx).put("format", "%Y-%m-%d %H:%M:%S");
                idx++;
                continue;
            } catch (ParseException e) {
                logger.debug(e.getMessage());
            }

            columns.get(idx).put("type", "string");
            idx++;
        }

        ConfigSource inputGuessed = config.deepCopy();
        inputGuessed.set("data.columns", columns);
        config.merge(inputGuessed);

        shutdown(consumer, executor);

        return Exec.newConfigDiff();
    }

    private ConsumerConfig getConsumerConfig(PluginTask task)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", task.getHost() + DELIMITER_DOMAIN_PORT + task.getPort());
        props.put("group.id", Exec.isPreview() ? UUID.randomUUID().toString() : task.getGroupId());
        props.put("zookeeper.session.timeout.ms", task.getZookeeperSessionTimeoutMs());
        props.put("zookeeper.sync.time.ms", task.getZookeeperSyncTimeMs());
        props.put("auto.commit.interval.ms", task.getAutoCommitIntervalMs());
        props.put("auto.offset.reset", task.getAutoOffsetReset());

        return new ConsumerConfig(props);
    }

    private List<KafkaStream<byte[], byte[]>> getStreams(PluginTask task, ConsumerConnector consumer)
    {
        HashMap<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(task.getTopic(), 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumer.createMessageStreams(topicCountMap);

        return consumerMap.get(task.getTopic());
    }

    private void shutdown(ConsumerConnector consumer,
                          ExecutorService executor)
    {
        consumer.shutdown();
        executor.shutdown();

        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                logger.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly");
        }
    }

}
