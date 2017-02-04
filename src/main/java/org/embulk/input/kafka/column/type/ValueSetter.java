package org.embulk.input.kafka.column.type;

/**
 * Base classFunction to set value to PageBuilder
 */

import org.embulk.spi.PageBuilder;

public interface ValueSetter {

    void setValue(PageBuilder builder, Object value, int columnIndex);

}
