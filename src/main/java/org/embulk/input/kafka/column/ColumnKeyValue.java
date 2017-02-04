package org.embulk.input.kafka.column;

import org.apache.kafka.common.serialization.*;
import org.embulk.input.kafka.column.type.*;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;


/**
 * Base class to handle input Key-Value data from KafkaBroker.
 */
public abstract class ColumnKeyValue extends ColumnBase {

    // Supported Serializers
    private final String STRING_DESERIALIZER = StringDeserializer.class.getName();

    private final String INTEGER_DESERIALIZER = IntegerDeserializer.class.getName();

    private final String LONG_DESERIALIZER = LongDeserializer.class.getName();

    private final String DOUBLE_DESERIALIZER = DoubleDeserializer.class.getName();

    private ValueSetter setter = null;


    protected ColumnKeyValue(String serializer) {
        if(serializer == null) {
            throw new NullPointerException("Deserializer should not be null.");
        } else if(STRING_DESERIALIZER.equals(serializer)) {
            this.type = Types.STRING;
            setter = new StringSetter();
        } else if(INTEGER_DESERIALIZER.equals(serializer)){
            this.type = Types.LONG;
            this.setter = new IntegerSetter();
        } else if(LONG_DESERIALIZER.equals(serializer)) {
            this.type = Types.LONG;
            this.setter = new LongSetter();
        } else if(DOUBLE_DESERIALIZER.equals(serializer)) {
            this.type = Types.DOUBLE;
            this.setter = new DoubleSetter();
        } else {
            throw new UnsupportedOperationException("Deserializer: " + serializer + " does not be supported in embulk-input-kafka.");
        }
    }

    protected void setPageBuilderToValue(PageBuilder builder, Object value, int columnIndex) {
        if(value == null) {
            builder.setNull(columnIndex);
        } else {
            setter.setValue(builder, value, columnIndex);
        }
    }
}
