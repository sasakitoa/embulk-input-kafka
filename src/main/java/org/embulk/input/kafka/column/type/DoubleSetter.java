package org.embulk.input.kafka.column.type;

import org.embulk.spi.PageBuilder;

/**
 * set value typed double to PageBuilder
 */
public class DoubleSetter implements ValueSetter {

    @Override
    public void setValue(PageBuilder builder, Object value, int columnIndex) {
        builder.setDouble(columnIndex, (Double)value);
    }

}
