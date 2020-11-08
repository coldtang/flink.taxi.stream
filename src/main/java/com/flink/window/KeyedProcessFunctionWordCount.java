package com.flink.window;

import com.flink.demo.WordCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 定时触发输出
 *
 * @author tang
 */
public class KeyedProcessFunctionWordCount {
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

        // Data Source
        // 从socket中读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, port);

        // Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        // non keyed stream
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordCount.WordOneFlatMapFunction());

        // 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes
                .keyBy(0);

        DataStream<Tuple2<String, Integer>> wordCounts = wordGroup
                //.process(new CountWithTimeoutFunction())
                // session window 的 gap 是静态的
                // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Integer>>() {
                    @Override
                    public long extract(Tuple2<String, Integer> element) {
                        if (element.f0.equals("this")) {
                            return 10000;
                        } else if (element.f0.equals("is")) {
                            return  20000;
                        }

                        return 5000;
                    }
                }))
                .sum(1);;

        // Data Sink
        wordCounts.print().setParallelism(1);

        // 启动并执行流程序
        env.execute("Streaming WordCount");
    }

    public static class CountWithTimeoutFunction
            extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>(
                    "myState", CountWithTimestamp.class
            ));
        }

        /**
         * 定时器需要运行的逻辑
         *
         * @param timestamp 定时器触发的时间戳
         * @param ctx       上下文
         * @param out       用于输出
         * @throws Exception
         */
        @Override
        public void processElement(Tuple2<String, Integer> timestamp,
                                   Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 拿到key的状态
            CountWithTimestamp currentState = state.value();
            if (currentState == null) {
                currentState = new CountWithTimestamp();
                currentState.key = timestamp.f0;
            }

            currentState.count++;
            // 更新这个 key 到达的时间，最后修改这个状态时间为当前的 Processing Time
            currentState.lastModified = ctx.timerService().currentProcessingTime();

            // 注册定时器
            ctx.timerService().registerProcessingTimeTimer(currentState.lastModified + 5000);


        }

        /**
         *  定时器需要运行的逻辑
         * @param timestamp 定时器触发的时间戳
         * @param ctx   上下文
         * @param out   用于输出
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            // 先拿到当前 key 的状态
            CountWithTimestamp curr = state.value();
            // 检查这个 key 是不是 5 秒钟没有接收到数据
            if (timestamp == curr.lastModified + 5000) {
                out.collect(Tuple2.of(curr.key, curr.count));
                state.clear();
            }
        }
    }

    private static class CountWithTimestamp {
        public String key;
        public int count;
        public long lastModified;
    }
}
