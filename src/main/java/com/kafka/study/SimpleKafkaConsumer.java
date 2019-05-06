package com.kafka.study;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @program: kafka-study
 * <p></p>
 * <p>
 * <PRE>
 * <BR>	修改记录
 * <BR>-----------------------------------------------
 * <BR>	修改日期			修改人			修改内容
 * </PRE>
 * @description:
 * @author: lijh99
 * @create: 2019-01-29 14:10
 * @version: V1.0
 **/

public class SimpleKafkaConsumer {
    private static KafkaConsumer<String,String> consumer;

    private final static String TOPIC = "mytest-topic";
    public SimpleKafkaConsumer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","10.211.55.4:9092");
        properties.put("group.id","test2");
        properties.put("enable.auto.commit","true");
        properties.put("auto.commit.interval.ms","1000");
        properties.put("session.timeout.ms","30000");
        properties.put("auto.offset.reset","earliest");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(properties);
    }

    public void consume(){
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record : records ) {
                System.out.printf("offset = %d,key = %s,value = %s",record.offset(),record.key(),record.value());

                System.out.println();
            }
        }
    }


    public static void main(String[] args) {
        new SimpleKafkaConsumer().consume();
    }
}
