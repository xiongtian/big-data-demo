package com.xiongtian.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/21 14:36
 */
public class CallBackProducer {
    public static void main(String[] args) {
        // 创建Kafka生产者得配置信息
        Properties properties = new Properties();
        // 指定连接的Kafka集群
        // kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.12.104:9092");
        //key,value的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 发送配置信息
        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<String, String>("first", 0, "xiongtian", "xiongtian--" + i),
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
