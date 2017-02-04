package org.embulk.input.kafka.column;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;

/**
 * Base class to set value from Kafka Broker to PageBuilder
 */
public abstract class ColumnBase {

    protected Type type;

    public abstract ColumnConfig getColumnConfig();

    public Type getType() {
        return this.type;
    }

    public abstract void setValue(PageBuilder builder, ConsumerRecord record, int columnIndex);

}
