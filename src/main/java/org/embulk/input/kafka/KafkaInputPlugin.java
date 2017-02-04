package org.embulk.input.kafka;

import java.util.*;

import com.google.common.base.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.embulk.spi.unit.ToStringMap;
import org.slf4j.Logger;

/**
 * Embulk plugin to get data from Apache Kafka
 */
public class KafkaInputPlugin implements InputPlugin {

    protected final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends Task {
        @Config("broker_list")
        public String getBrokerList();

        @Config("topics")
        public List<String> getTopics();

        @Config("key_deserializer")
        @ConfigDefault("\"\"")
        public String getKeyDeserializer();

        @Config("value_deserializer")
        @ConfigDefault("\"org.apache.kafka.common.serialization.StringDeserializer\"")
        public String getValueDeserializer();

        @Config("columns")
        @ConfigDefault("[key, value]")
        public List<String> getColumns();

        @Config("load_from_beginning")
        @ConfigDefault("false")
        public Boolean getLoadFromBeginning();

        @Config("seek")
        public Optional<List<ToStringMap>> getSeek();

        @Config("poll_timeout_sec")
        @ConfigDefault("3")
        public int getPollTimeoutSec();

        @Config("extra_kafka_options")
        @ConfigDefault("null")
        public Optional<ToStringMap> getExtraKafkaOptions();

        @Config("num_tasks")
        @ConfigDefault("1")
        public Integer getNumTasks();

        @Config("kafka_group_id")
        @ConfigDefault("\"EmbulkConsumer\"")
        public String getKafkaGroupId();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        KafkaInputColumns columns = new KafkaInputColumns(task);
        Schema schema = columns.getSchema();

        return resume(task.dump(), schema, task.getNumTasks(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control) {
        control.run(taskSource, schema, taskCount);
        ConfigDiff conf = Exec.newConfigDiff();
        return conf;
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
        // Nothing to do
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder builder = new PageBuilder(allocator, schema, output);
        KafkaInputColumns columns = new KafkaInputColumns(task);

        KafkaProperties props = new KafkaProperties(task);
        KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(task.getTopics());
        setOffsetPosition(consumer, task);

        long readRecords = 0;
        long showReadRecords = 500;
        while(true) {
            ConsumerRecords<?,?> records = consumer.poll(task.getPollTimeoutSec() * 1000);
            if(records.count() == 0) {
                break;
            }
            readRecords += records.count();
            columns.setOutputRecords(builder, records);
            builder.flush();
            if(readRecords >= showReadRecords) {
                logger.info(String.format("Read %d record(s) in task-%d", readRecords, taskIndex));
                showReadRecords *= 2;
            }
        }
        builder.close();
        logger.info(String.format("Finishing task-%d.Total %d record(s) read in this task", taskIndex, readRecords));
        consumer.close();

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config) {
        return Exec.newConfigDiff();
    }

    private void setOffsetPosition(KafkaConsumer<?, ?> consumer, PluginTask task) {
        // Set all offset belongs subscribing topics to beginning
        if(task.getLoadFromBeginning()) {
            for(String topic : task.getTopics()) {
                List<TopicPartition> tpList = new LinkedList<>();
                for(PartitionInfo info :consumer.partitionsFor(topic)) {
                    tpList.add(new TopicPartition(info.topic(), info.partition()));
                }
                consumer.seekToBeginning(tpList);
                logger.debug(String.format("All partition(s) of topic \"%s\" were set offset beginning."));
            }
            logger.info("All partition(s) of subscribing topics were set offset beginning.");

            if(task.getSeek().isPresent()) {
                logger.info("Some offset position will reconfigure because 'seek' option is specified.");
            }
        }

        // Set offset position specified in seek option
        if(task.getSeek().isPresent()) {
            for(ToStringMap conf : task.getSeek().get()) {
                if(!conf.containsKey("topic") || !conf.containsKey("partition") || !conf.containsKey("offset")) {
                    throw new IllegalArgumentException("All of 'topic', 'partition', 'offset' should be specified in seek option.");
                }
                try {
                    String topic = conf.get("topic");
                    int partition = Integer.parseInt(conf.get("partition"));
                    long offset = Long.parseLong(conf.get("offset"));
                    consumer.seek(new TopicPartition(topic, partition), offset);
                    logger.debug(String.format("Seek: Topic:%s Partition:%d Offset:%d", topic, partition, offset));
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("'partition' and 'offset' in seek option should be integer.");
                }
            }
        }
    }
}
