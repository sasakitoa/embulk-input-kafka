# Kafka input plugin for Embulk

Kafka input Plugin is plugin for Embulk to load data from Apache Kafka.

## Overview

| Item              | Description |
|:------------------|:------------|
| Plugin type       | input       |
| Resume supported  | no          |
| Cleanup supported | no          |
| Guess supported   | no          |

## Configuration

| Name                | Description                                                                     | Required |  default Value                                           |
|:--------------------|:--------------------------------------------------------------------------------|:---------|:---------------------------------------------------------|
| broker_list         | Address to initial connection to Kafka cluster.                                 | yes      | -                                                        |
| topics              | List of topics to load data. You can specify multi topics to use conma          | yes      | -                                                        |
| key_deserializer    | A Class to deserialize key of messages                                          | no       | org.apache.kafka.common.serialization.StringDeserializer |
| value_deserializer  | A Class to deserialize value of messages                                        | no       | org.apache.kafka.common.serialization.StringDeserializer |
| columns             | Columns to fecth. (See blow setion for details)                                 | no       | [key, value]                                             |
| load_from_beginning | If true, load data from beginning of all parititons of subscribing topics.      | no       | load_from_beginning                                      |
| seek                | If set, set fetch offsets of partitions specified value before loading data.    | no       | -                                                        |
| extra_kafka_options | Adding properties to KafkaConsumer                                              | no       | -                                                        |
| num_tasks           | Number of task. if num_task=5, 5 consumer will be created                       | no       | 1                                                        |
| kafka_group_id      | Identifies the consumer group. If num_task > 1, all consumer use this value     | no       | EmbulkConsumer                                           |
| poll_timeout_sec    | Timeout for KafkaConsumer#poll method                                           | no       | 3                                                        |

### Columns properties

You should specify values of `column` in below table to columns in configuration.

| column    | Type                                     | Note                    |
|:----------|:-----------------------------------------|:------------------------|
| key       | auto determined by 'key_deserializer'    |                         |
| value     | auto determined by 'value_deserializer'  |                         |
| topic     | string                                   |                         |
| partition | long                                     |                         |
| offset    | long                                     |                         |
| timestamp | long                                     | UnixTime in millisecond |


## Example

```yaml
in:
  type: kafka
  broker_list: kafkahost1:9092,kafkahost2:9092,kafkahost3:9092
  topics:
    - topic1
    - topic2
  columns:
    - key
    - value
    - partition
    - offset
  load_from_beginning: true
  seek:
    - { topic: topic1, partition: 2, offset: 20 }
    - { topic: topic1, partition: 3, offset: 25 }
  extra_kafka_options:
    auto.offset.reset: earliest
    enable.auto.commit: false
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
