package com.flink.project;

import com.flink.DataFilePath;
import com.flink.datatypes.TaxiFare;
import com.flink.source.GzpFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 需求：实时计算每隔一个小时赚钱最多的司机
 * 1. 计算出每个小时每个司机总共赚了多少钱
 * 2. 计算出赚钱最多的司机
 */
public class HourlyTips implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        DataStream<Tuple2<Long, Float>> source = env.addSource(new GzpFileSource(TAXI_FARE_PATH))
                .map(new MapFunction<String, Tuple2<Long, Float>>() {
                    @Override
                    public Tuple2<Long, Float> map(String line) throws Exception {
                        TaxiFare fare = TaxiFare.fromString(line);
                        return Tuple2.of(fare.getDriverId(), fare.getTip());
                    }
                });
        // 计算出每个小时每个司机总共赚了多少钱
        DataStream<Tuple2<Long, Float>> tips = source.keyBy(fare -> fare.f0).timeWindow(Time.hours(1))
                .process(new ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context context,
                                        Iterable<Tuple2<Long, Float>> elements,
                                        Collector<Tuple2<Long, Float>> out) throws Exception {
                        float sum = 0F;
                        for (Tuple2<Long, Float> fare : elements) {
                            sum += fare.f1;
                        }
                        out.collect(Tuple2.of(key, sum));
                    }
                });

        // 计算最赚钱的司机
        // 合并窗口
        DataStream<Tuple2<Long, Float>> maxTip = tips.timeWindowAll(Time.hours(1))
                .maxBy(1);

        maxTip.print();

        env.execute("HourlyTips");
    }
}
