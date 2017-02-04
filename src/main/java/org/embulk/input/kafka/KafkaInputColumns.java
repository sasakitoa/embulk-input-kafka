package org.embulk.input.kafka;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.embulk.input.kafka.column.*;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

import java.util.LinkedList;
import java.util.List;

/**
 * handle input columns for KafkaInputPlugin
 */
public class KafkaInputColumns {

    private final static String[] columnList = {"key", "value", "topic", "partition", "offset", "timestamp"};

    private ColumnBase[] columnHandler;

    private Schema schema;

    public KafkaInputColumns(KafkaInputPlugin.PluginTask task) {

        List<String> columns = task.getColumns();

        // Validate column name
        if(columns.size() <= 0) {
            throw new IllegalArgumentException("columns property should have one or more elements.");
        }
        for(String column : columns) {
            if(column == null) {
                throw new IllegalArgumentException("column name should not be null.");
            }
            if(!ArrayUtils.contains(columnList, column.toLowerCase())) {
                throw new IllegalArgumentException("column: \"" + column + "\" does not use in columns property.");
            }
        }

        columnHandler = new ColumnBase[columns.size()];
        List<ColumnConfig> columnConfigs = new LinkedList<>();
        for(int i = 0; i < columnHandler.length; i++) {
            columnHandler[i] = getHandler(columns.get(i), task);
            columnConfigs.add(columnHandler[i].getColumnConfig());
        }

        this.schema = new SchemaConfig(columnConfigs).toSchema();
    }

    public Schema getSchema() {
        if(schema == null) {
            throw new NullPointerException("KafkaInputColumns was not initialized.");
        }
        return this.schema;
    }

    public void setOutputRecords(PageBuilder builder, ConsumerRecords<?, ?> records) {
        for(ConsumerRecord record: records) {
            for(int c = 0; c < columnHandler.length; c++) {
                columnHandler[c].setValue(builder, record, c);
            }
            builder.addRecord();
        }
    }

    private ColumnBase getHandler(String column, KafkaInputPlugin.PluginTask task) {
        switch (column.toLowerCase()) {
            case "key":
                return new ColumnKey(task);
            case "value":
                return new ColumnValue(task);
            case "topic":
                return new ColumnTopic();
            case "partition":
                return new ColumnPartition();
            case "offset":
                return new ColumnOffset();
            case "timestamp":
                return new ColumnTimestamp();
            default:
                throw new IllegalArgumentException("column name " + column + "cloud not use here.");
        }
    }
}
