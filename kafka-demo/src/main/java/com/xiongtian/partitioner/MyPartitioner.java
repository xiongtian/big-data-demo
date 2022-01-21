package com.xiongtian.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author xiongtian
 * @version 1.0
 * @date 2022/1/21 15:00
 */
public class MyPartitioner implements Partitioner {


    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // TODO 在这里写分区的代码
        return 1;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
