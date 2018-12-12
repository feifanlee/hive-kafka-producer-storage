package com.github.feifanlee.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;

import java.util.Map;
import java.util.Properties;

/**
 * Created by lifeifan on 2018/12/10.
 */
public class KafkaProducerStorageHandler extends DefaultStorageHandler{
    private Configuration conf;

    @Override
    public void setConf(Configuration _conf) {
        this.conf = _conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        this.configureTableJobProperties(tableDesc,jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        this.configureTableJobProperties(tableDesc,jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties tableProperties = tableDesc.getProperties();
        jobProperties.put(
                KafkaUtil.PRO_KEY_BROKER,
                tableProperties.getProperty(KafkaUtil.PRO_KEY_BROKER)
        );
        jobProperties.put(
                KafkaUtil.PRO_KEY_TOPIC,
                tableProperties.getProperty(KafkaUtil.PRO_KEY_TOPIC)
        );
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return JsonSerDe.class;
    }

    @Override
    public Class<? extends org.apache.hadoop.mapred.OutputFormat> getOutputFormatClass() {
        return KafkaProducerOutputFormat.class;
    }
}
