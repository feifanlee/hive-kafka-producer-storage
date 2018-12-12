package com.github.feifanlee.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by lifeifan on 2018/12/10.
 */
public class KafkaProducerOutputFormat implements OutputFormat<Writable,Object> {
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {


        Properties props = new Properties();
        props.put(KafkaUtil.PRO_KEY_BROKER,jobConf.get(KafkaUtil.PRO_KEY_BROKER));
        props.put(KafkaUtil.PRO_KEY_TOPIC,jobConf.get(KafkaUtil.PRO_KEY_TOPIC));
        return new KafkaRecordWriter(props);
    }

    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }

    static private class KafkaRecordWriter implements RecordWriter<Writable,Object>{
        String topic;
        Producer<String,String> producer;

        public KafkaRecordWriter(Properties props){
            props = KafkaUtil.getProducerProperties(props);
            this.producer = KafkaUtil.getProducer(props);
            this.topic = props.getProperty(KafkaUtil.PRO_KEY_TOPIC);
        }

        public void write(Writable key, Object value) throws IOException {
            String msg = value.toString();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, msg);
            producer.send(record);
        }

        public void close(Reporter reporter) throws IOException {
        }
    }
}
