package com.github.ash3798.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String , String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0 ; i< 10 ; i++) {

            String topic = "second-topic";
            String value = "hello world "+ Integer.toString(i);
            String key = "id_" + i;

            //create a producer record
            ProducerRecord<String,String> record = new ProducerRecord<>(topic ,key, value);

            logger.info(key);

            //send the record - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        logger.error("error while producing , Error :" + e);
                    }else{
                        logger.info("Record produced successfully. details : \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Offset: " + recordMetadata.offset()+ "\n" +
                                "Partition: " + recordMetadata.partition() );
                    }
                }
            });
        }


        //flush the record
        producer.flush();

        //flush and close
        producer.close();
    }
}
