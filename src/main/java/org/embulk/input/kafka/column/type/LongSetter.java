package org.embulk.input.kafka.column.type;

import org.embulk.spi.PageBuilder;

/**
 * Function to set Long value to PageBuilder
 */
public class LongSetter implements ValueSetter {

    @Override
    public void setValue(PageBuilder builder, Object value, int columnIndex) {
        builder.setLong(columnIndex, (Long)value);
    }

}
