package com.xiongtian.apitest.transform;

import com.xiongtian.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by xiongtian on 2022/2/11 10:30
 */
public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\mycode\\java_code\\big-data-demo\\flink-demo\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });
        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream =
                dataStream.keyBy("id");
        //KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，取当前最大的温度值
        //SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature"); 使用max只会改变sensor对象中温度这一个属性
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature"); //使用maxBy其他属性也会更改

        resultStream.print();

        env.execute();
    }
}
