package com.flink.window;

import com.flink.demo.WordCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * key or non-keyd window
 *
 * @author tang
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String ip;
        try {
            ip = parameterTool.get("ip");
        } catch (Exception e) {
            System.err.println("需要提供 --ip 参数");
            return;
        }

        int port;
        try {
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("需要提供 --port 参数");
            return;
        }

        // 初始化一个流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        // 本地启动的时候，默认的并行度等于你的电脑的核心数
        // Flink 程序是有一个默认的最大并行度，默认值是 128
        // 可以设置最大并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Data Source
        // 从socket中读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip,port);

        // Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        // non keyed stream
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordCount.WordOneFlatMapFunction());
        // non keyed window ：不能设置并行度的
        // 每隔 3 秒钟计算所有单词的个数
        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> nonKeyedWindow =
                wordOnes.timeWindowAll(Time.seconds(3));

        // 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes
                .keyBy(0);
        // keyed window ；可以设置并行度
        // 每隔 3 秒钟计算每个单词出现的次数
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> keyedWindow = wordGroup
                .timeWindow(Time.seconds(3));

        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        // Data Sink
        wordCounts.print().setParallelism(1);

        // 启动并执行流程序
        env.execute("Streaming WordCount");
    }
}
