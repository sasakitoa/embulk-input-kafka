package org.embulk.input.kafka.column;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;

/**
 * Base class to set Topic record from Kafka Broker to PageBuilder
 */
public class ColumnTopic extends ColumnBase {

    public ColumnTopic() {
        this.type = Types.STRING;
    }

    @Override
    public ColumnConfig getColumnConfig() {
        return new ColumnConfig("topic", Types.STRING, "%s");
    }

    @Override
    public void setValue(PageBuilder builder, ConsumerRecord record, int columnIndex) {
        builder.setString(columnIndex, record.topic());
    }
}
