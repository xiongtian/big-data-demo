package com.xiongtian.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/20 20:55
 */
public class PartitionProducer {
    public static void main(String[] args) {

        // 1、创建Kafka生产者得配置信息
        Properties properties = new Properties();

        // 2、指定连接的Kafka集群
        // kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.12.104:9092");

        // 3、指定ACK应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // 4、重试次数
        properties.put("retries", 3);

        // 5、批次大小
        properties.put("batch.size", 16384);

        // 6、等待时间
        properties.put("linger.ms", 1);

        // 7、RecordAccumulator缓冲区的大小
        properties.put("buffer.memory", 33554432);

        // 8、key,value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.xiongtian.partitioner.MyPartitioner");

        // 9、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 发送配置信息
        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<String, String>("first", "xiongtian--" + i),
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (null == exception) {
                                System.out.println(metadata.partition() + "--" + metadata.offset());
                            } else {
                                exception.printStackTrace();
                            }
                        }
                    });
        }

        // 释放资源
        producer.close();


    }
}
