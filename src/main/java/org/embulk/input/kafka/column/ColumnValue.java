package org.embulk.input.kafka.column;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.embulk.input.kafka.KafkaInputPlugin;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.PageBuilder;

/**
 * Created by sasaki on 17/01/24.
 */
public class ColumnValue extends ColumnKeyValue {

    public ColumnValue(KafkaInputPlugin.PluginTask task) {
        super(task.getValueDeserializer());
    }

    @Override
    public ColumnConfig getColumnConfig() {
        return new ColumnConfig("value", this.type, "%s");
    }

    @Override
    public void setValue(PageBuilder builder, ConsumerRecord record, int columnIndex) {
        setPageBuilderToValue(builder, record.value(), columnIndex);
    }
}
