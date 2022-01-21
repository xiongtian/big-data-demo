package com.xiongtian.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.security.auth.login.Configuration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/21 16:08
 */
public class MyConsumer {
    public static void main(String[] args) {

        // 1、创建消费者配置信息
        Properties properties = new Properties();

        // 2、给配置信息赋值

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.12.104:9092"); // 连接的集群
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // 开启自动提交
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 自动提交的延迟

        // key,value 的反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题
        consumer.subscribe(Collections.singletonList("first"));

        // 获取数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            // 解析并打印ConsumerRecords
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
            }
        }


        // 关闭资源
        //consumer.close();
    }
}
