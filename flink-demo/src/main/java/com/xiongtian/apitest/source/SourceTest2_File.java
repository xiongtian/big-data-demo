package com.xiongtian.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by xiongtian on 2022/2/10 11:34
 */
// 从
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\mycode\\java_code\\big-data-demo\\flink-demo\\src\\main\\resources\\sensor.txt");

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
