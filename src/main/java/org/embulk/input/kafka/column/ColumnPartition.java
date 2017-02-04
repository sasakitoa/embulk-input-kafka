package org.embulk.input.kafka.column;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;

/**
 * Base class to set Partition record from Kafka Broker to PageBuilder
 */
public class ColumnPartition extends ColumnBase {

    public ColumnPartition() {
        this.type = Types.LONG;
    }

    @Override
    public ColumnConfig getColumnConfig() {
        return new ColumnConfig("partition", this.type, "%d");
    }

    @Override
    public void setValue(PageBuilder builder, ConsumerRecord record, int columnIndex) {
        builder.setLong(columnIndex, (long)record.partition());
    }
}
