package com.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

/**
*
*  统计单词数量
*  author：tang
*
* */
public class WorldCount {
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

        // 初始化流执行环境
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        // 本地并行度等于电脑内核数

        env.setMaxParallelism(8);

        env.setParallelism(2);
        // Data Source
        // 从socket读取数据
        DataStreamSource<String> worldString = env.socketTextStream(ip, port);
        // Data Process
        // 格式化
        DataStream<Tuple2<String, Integer>> source = worldString.flatMap(new WordOneFlatMapFunction());
        // 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = source.keyBy(0);
        //聚合计算
        DataStream<Tuple2<String, Integer>> wordCounts=wordGroup.sum(1);
        // Data Sink
        wordCounts.print();
        // 启动并执行流程序
        env.execute("Streaming WorldCount");
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] worlds = line.toLowerCase().split(" ");
            for (String world : worlds) {
                Tuple2<String, Integer> wordInfo = new Tuple2<>(world, 1);
                // 输出
                out.collect(wordInfo);
            }
        }
    }
}
