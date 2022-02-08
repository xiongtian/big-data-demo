package com.xiongtian.wc;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by xiongtian on 2022/2/8 21:12
 */

// 流处理 word count
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(3);

        // 从文件中读取数据
        String inputPath = "D:\\mycode\\java_code\\big-data-demo\\flink-demo\\src\\main\\resources\\hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStram = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0) // 按照当前
                .sum(1);

        resultStram.print();

        // 执行任务
        // 4> (are,1): ">" 前面的数字4,表示的是执行的并行度，与开发环境机器的内核数有关
        env.execute();
    }
}
