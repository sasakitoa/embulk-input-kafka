package org.embulk.input.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.Properties;

/**
 * set KafkaConsumer configs from PluginTask
 */
public class KafkaProperties extends Properties {

    protected final Logger logger = Exec.getLogger(getClass());

    public KafkaProperties(KafkaInputPlugin.PluginTask task) {
        // Set user specified extra options
        if(task.getExtraKafkaOptions().isPresent()) {
            this.putAll(task.getExtraKafkaOptions().get().toProperties());
        }

        if(this.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            logger.warn(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + " in extra_kafka_options does not affect."
                    + "you should use key_deserializer instead.");
        }
        if(this.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            logger.warn(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + " in extra_kafka_options does not affect."
                    + " you should use value_deserializer instead.");
        }

        this.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokerList());
        this.put(ConsumerConfig.GROUP_ID_CONFIG, task.getKafkaGroupId());
        this.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, task.getKeyDeserializer());
        this.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, task.getValueDeserializer());
    }
}
