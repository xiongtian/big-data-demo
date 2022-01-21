package com.xiongtian.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/21 18:47
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    public void configure(Map<String, ?> configs) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 1、取出数据
        String value = record.value();

        // 创建一个对象并返回
        return new ProducerRecord<String, String>(record.topic(), record.partition(), record.key(), System.currentTimeMillis() + "," + value);
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }


}
