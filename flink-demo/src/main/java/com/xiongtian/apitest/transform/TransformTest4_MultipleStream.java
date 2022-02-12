package com.xiongtian.apitest.transform;

import com.xiongtian.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * Created by xiongtian on 2022/2/12 14:39
 */
public class TransformTest4_MultipleStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\mycode\\java_code\\big-data-demo\\flink-demo\\src\\main\\resources\\sensor.txt");

        // 转换成为SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1.分流操作，按照温度30度为界，分为两个流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempStream.print("all");


        env.execute();

    }
}
