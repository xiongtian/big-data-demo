package com.xiongtian.apitest.transform;

import com.xiongtian.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by xiongtian on 2022/2/12 10:15
 */
public class TransformTest3_Reduce {

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

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimeStamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });
        reduce.print();

        env.execute();
    }
}
