package com.github.ash3798.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerReplyAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServer = "127.0.0.1:9092" ;
        String groupID = "my-application";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //create a consumer
        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

        //assign topic and partition to consumer
        String topicName = "second-topic";
        Long offsetToReadFrom = 10L;

        TopicPartition partition = new TopicPartition(topicName , 0);
        consumer.assign(Arrays.asList(partition));

        //seek
        consumer.seek(partition , offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int numberOfMessagesRead = 0;

        //read the data from topic
        while(keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records){
                numberOfMessagesRead += 1;
                logger.info("Record received =>" + " Topic: " + record.topic() + " partition: " + record.partition() + " key: " + record.key() + " value: " + record.value());

                if (numberOfMessagesRead == numberOfMessagesToRead) {
                    keepReading = false;
                    break;
                }
            }

        }
    }
}
