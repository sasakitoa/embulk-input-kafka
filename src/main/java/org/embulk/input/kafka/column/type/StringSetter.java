package org.embulk.input.kafka.column.type;

import org.embulk.spi.PageBuilder;

/**
 * Function to set String value to PageBuilder
 */
public class StringSetter implements ValueSetter {

    @Override
    public void setValue(PageBuilder builder, Object value, int columnIndex) {
        builder.setString(columnIndex, (String)value);
    }

}
