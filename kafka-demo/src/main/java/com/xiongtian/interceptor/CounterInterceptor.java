package com.xiongtian.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/21 19:00
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    int success;
    int error;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null != metadata) {
            success++;
        } else {
            error++;
        }
    }

    public void close() {

        System.out.println("success:"+success);
        System.out.println("error:"+error);
    }

    public void configure(Map<String, ?> configs) {

    }
}
