package com.xiongtian.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.naming.MalformedLinkException;
import java.util.Properties;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/20 20:55
 */
public class MyProducer {
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

        // 9、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 10、发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", "atguigu--" + i));
        }

        // 11、关闭资源
        producer.close();

    }
}
