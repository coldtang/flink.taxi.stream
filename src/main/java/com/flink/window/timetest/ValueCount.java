package com.flink.window.timetest;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.EnumBiMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
* 统计值数量(enventTime 处理乱序)
 * @author tang
* */
public class ValueCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 默认的情况， Flink 使用 Processing Time
        // 设置使用 Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 默认的话 watermark 产生的周期是 200 ms
        // 设置 watermark 产生的周期为 1000ms
        env.getConfig().setAutoWatermarkInterval(1000);

        env.addSource(new TestSource()).map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] strings = line.split(",");
                return Tuple2.of(strings[0], Long.valueOf(strings[1]));
            }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor()) // 设置获取 Event Time 的逻辑
        .keyBy(0)
        .timeWindow(Time.seconds(10),Time.seconds(5))
        .process(new ValueCountProcessWindowFunction())
        .print();

        env.execute("Flink ValueCount");
    }

    private static class EventTimeExtractor
            implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        // 拿到每一个事件的 Event Time
        @Override
        public long extractTimestamp(Tuple2<String, Long> element,
                                     long previousElementTimestamp) {
            //System.out.println("current element is : " + element);
            //System.out.println("current event time is : " + dateFormat.format(element.f1));
            return element.f1;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //System.out.println("generate watermark at : " + System.currentTimeMillis());
            // 延迟 5 秒触发计算 window
            return new Watermark(System.currentTimeMillis() - 5000);
        }
    }

    private static class ValueCountProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<Tuple2<String, Long>> out) throws Exception {
            System.out.println("当前时间：" + dateFormat.format(System.currentTimeMillis()));
            System.out.println("处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("start : " + dateFormat.format(context.window().getStart()));
            long count = Iterables.size(elements);
            out.collect(Tuple2.of(tuple.getField(0), count));
            System.out.println("end : " + dateFormat.format(context.window().getEnd()));
        }
    }
}
