package com.github.feifanlee.hive;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lifeifan on 2018/12/11.
 */
public class KafkaUtil {

    private static Producer<String,String> producer;

    public synchronized static Producer<String,String> getProducer(Properties props){
        if(null == producer) {
            producer = new KafkaProducer(props);
        }
        return producer;
    }

    public final static String PRO_KEY_BROKER = "bootstrap.servers";
    public final static String PRO_KEY_TOPIC = "topic";

    public static Properties getProducerProperties(Properties props){
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
    public static Properties getProducerProperties(Map<String,String> map){
        Properties props = new Properties();
        props = getProducerProperties(props);

        props.put(PRO_KEY_BROKER,map.get(PRO_KEY_BROKER));
        props.put(PRO_KEY_TOPIC,map.get(PRO_KEY_TOPIC));

        return props;
    }

    public static void main(String[] args) throws Exception{
        String brokers = "192.168.1.58:9092";
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("topic","lfftest");

        for(int i=0; i<10; i++){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("lfftest", null, new Date().toString());
            getProducer(props).send(record);
            Thread.sleep(1000);
        }
    }

}
