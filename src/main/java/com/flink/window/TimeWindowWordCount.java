package com.flink.window;

import org.apache.commons.lang3.time.FastDateFormat;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  统计单词数量
 *  @author：tang
 * */
public class TimeWindowWordCount {
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
        DataStreamSource<String> wordString = env.socketTextStream(ip, port);
        // Data Process
        // 格式化
        DataStream<Tuple2<String, Integer>> source = wordString.flatMap(new WordOneFlatMapFunction());
        // 分组
        DataStream<Tuple2<String, Integer>> wordCounts =
                source.keyBy(0)
                        .timeWindow(Time.seconds(10),Time.seconds(5))
                        .process(new SumProcessWindowFunction());
        // Data Sink
        wordCounts.print();
        // 启动并执行流程序
        env.execute("Streaming WordCount");
    }

    private static class SumProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

        private FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        /**
         *  当 window 触发计算的时候会调用这个方法
         *  处理一个 window 中单个 key 对应的所有的元素
         *
         *  window 中的每一个 key 会调用一次
         *
         * @param tuple  key
         * @param context   operator 的上下文
         * @param elements  指定 window 中指定 key 对应的所有的元素
         * @param out   用于输出
         * @throws Exception
         */
        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("-----------------------------------------------------");
            System.out.println("当前系统的时间：" + dateFormat.format(System.currentTimeMillis()));
            System.out.println("window 处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window 的开始时间：" + dateFormat.format(context.window().getStart()));
            // 每个单词在指定的 window 中出现的次数
            int sum = 0;
            for (Tuple2<String, Integer> ele : elements) {
                sum += 1;
            }
            // 输出单词出现的次数
            out.collect(Tuple2.of(tuple.getField(0), sum));
            System.out.println("window 的结束时间：" + dateFormat.format(context.window().getEnd()));
        }
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{
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
