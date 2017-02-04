package org.embulk.input.kafka.column;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.embulk.input.kafka.KafkaInputPlugin;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.PageBuilder;

/**
 * Base class to set Key record from Kafka Broker to PageBuilder
 */
public class ColumnKey extends ColumnKeyValue {

    public ColumnKey(KafkaInputPlugin.PluginTask task) {
        super(task.getKeyDeserializer());
    }

    @Override
    public ColumnConfig getColumnConfig() {
        return new ColumnConfig("key", this.type, "%s");
    }

    @Override
    public void setValue(PageBuilder builder, ConsumerRecord record, int columnIndex) {
        setPageBuilderToValue(builder, record.key(), columnIndex);
    }
}
