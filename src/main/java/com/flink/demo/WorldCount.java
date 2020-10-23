package com.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

public class WorldCount {
    public static void main(String[] args) throws Exception {
        // 初始化流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Data Source
        // 从socket读取数据
        DataStreamSource<String> worldString = env.socketTextStream("localhost", 5001);
        // Data Process
        // 格式化
        DataStream<Tuple2<String, Integer>> source = worldString.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] worlds = line.toLowerCase().split(" ");
                for (String world : worlds) {
                    Tuple2<String, Integer> wordInfo = new Tuple2<>(world, 1);
                    // 输出
                    out.collect(wordInfo);
                }
            }
        });
        // 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = source.keyBy(0);
        //聚合计算
        DataStream<Tuple2<String, Integer>> wordCounts=wordGroup.sum(1);
        // Data Sink
        wordCounts.print();
        // 启动并执行流程序
        env.execute("Streaming WorldCount");
    }
}
