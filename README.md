#hive-kafka-producer-storage

Load hive table to kafka by insert operation



add hive-kafka-producer-storage.jar (maybe also kafka-clients.jar) into hive_aux_path
restart hiveserver2

create external hive table

```sql
CREATE EXTERNAL TABLE `lffkafka`(                  
   `str` string ,        
   `num` int )           
 STORED BY                                          
   'com.github.feifanlee.hive.KafkaProducerStorageHandler'  
 WITH SERDEPROPERTIES (                             
   'bootstrap.servers'='192.168.1.58:9092',                 
   'topic'='lfftest');
```

then insert into this table
```sql
INSERT INTO TABLE lffkafka SELECT str,num from lff;
```

