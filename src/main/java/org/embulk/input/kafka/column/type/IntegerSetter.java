package org.embulk.input.kafka.column.type;

import org.embulk.spi.PageBuilder;

/**
 * Function to set Integer value to PageBuilder
 */
public class IntegerSetter implements ValueSetter{

    @Override
    public void setValue(PageBuilder builder, Object value, int columnIndex) {
        builder.setLong(columnIndex, new Long((int)value));
    }
}
