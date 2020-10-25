package com.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountKafka {
    public static void main(String[] args) throws Exception {
        // 初始化流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(8);
        env.setParallelism(2);
        // Data Source
        // 从kafka读取数据
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "master:9092");
        properties.setProperty("group.id", "flink-test");
        FlinkKafkaConsumer<String> wordKafaString=
                new FlinkKafkaConsumer<String>("flink-input",new SimpleStringSchema(),properties);
        DataStreamSource<String> wordString = env.addSource(wordKafaString,"kafkaSource");
        // Data Process
        // 格式化
        DataStream<Tuple2<String, Integer>> source = wordString.flatMap(new WordOneFlatMapFunction());

        DataStream<Tuple2<String, Integer>> wordCounts =
                source.keyBy(0).sum(1);

        // Data Sink
        FlinkKafkaProducer<String> producer=
                new FlinkKafkaProducer<String>("master:9092","flink-output",new SimpleStringSchema());
        wordCounts.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.toString();
            }
        }).addSink(producer).name("kafkaSink");
        // 启动并执行流程序
        env.execute("Streaming WordCount");
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.toLowerCase().split(" ");
            for (String word : words) {
                Tuple2<String, Integer> wordInfo = new Tuple2<>(word, 1);
                // 输出
                out.collect(wordInfo);
            }
        }
    }
}
