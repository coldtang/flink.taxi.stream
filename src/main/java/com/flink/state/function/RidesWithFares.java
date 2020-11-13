package com.flink.state.function;

import com.flink.DataFilePath;
import com.flink.datatypes.TaxiFare;
import com.flink.datatypes.TaxiRide;
import com.flink.source.GzpFileSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.zip.GZIPInputStream;

/**
 * 根据 rideId 关联 TaxiRide 和 TaxiFare
 *
 * @author tang
 */
public class RidesWithFares implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 三种 StateBackend
        // 默认的话是 5 M
        // MemoryStateBackend memoryStateBackend = new MemoryStateBackend(100 * 1024 * 1024);
        // FsStateBackend fsStateBackend = new FsStateBackend("hdfs://master:9001/checkpoint-path/");
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://master:9001/checkpoint-path/");

        env.setStateBackend(rocksDBStateBackend);

        // 设置 checkpoint
        // 开启 checkpoint 功能，checkpoint 的周期是 10 秒
        env.enableCheckpointing(10000);
        // 配置 checkpoint 行为特性
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 设置两个 checkpoint 之间必须间隔一段时间
        // 设置两个 checkpoint 之间最小间隔时间是 30 秒
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        // 设置可以允许多个 checkpoint 一起运行，前提是 checkpoint 不占资源
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        // 可以给 checkpoint 设置超时时间，如果达到了超时时间的话，Flink 会强制丢弃这一次 checkpoint
        // 默认值是 10 分钟
        checkpointConfig.setCheckpointTimeout(30000);
        // 设置即使 checkpoint 出错了，继续让程序正常运行
        // 1.9.0 不建议使用这个参数
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 设置当 flink 程序取消的时候保留 checkpoint 数据
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置 Flink 程序的自动重启策略
        // 默认：fixed-delay restart strategy，重启的次数是 Integer.MAX_VALUE，重启之间的时间间隔是 10秒
        // 1. fixed-delay restart strategy ：尝试重启多次，每次重启之间有一个时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5, // 重新启动总次数
                Time.seconds(30) // 每次重新启动的时间间隔
        ));
        // 2. failure-rate restart strategy ：
        // 尝试在一个时间段内重启执行次数，每次重启之间也需要一个时间间隔的
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5, // 在指定时段重启的次数
                Time.seconds(30), // 指定的时间段
                Time.seconds(5) // 两次重启之间的时间间隔
        ));
        // 3. no-restart strategy：不进行重启，一旦发生错误立马停止程序
        env.setRestartStrategy(RestartStrategies.noRestart());

        //读取 TaxiRide 数据
        KeyedStream<TaxiRide, Long> rides = env.addSource(new GzpFileSource(TAXI_RIDE_PATH))
                .map(m -> TaxiRide.fromString(m))
                .keyBy(ride -> ride.getRideId());

        //读取 TaxiFare 数据
        KeyedStream<TaxiFare, Long> fares = env.addSource(new GzpFileSource(TAXI_FARE_PATH))
                .map(m -> TaxiFare.fromString(m))
                .keyBy(fare -> fare.getRideId());

        rides
                .connect(fares)
                .flatMap(new EnrichmentFunction())
                .addSink(new CustomSink(20));


        env.execute("RidesWithFares");
    }

    public static class EnrichmentFunction
            extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // 记住相同的 rideId 对应的 taxi ride 事件
        private ValueState<TaxiRide> rideValueState;
        // 记住相同的 rideId 对应的 taxi fare 事件
        private ValueState<TaxiFare> fareValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            rideValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<TaxiRide>("save ride", TaxiRide.class));

            fareValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<TaxiFare>("save fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide taxiRide,
                             Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            //这里是处理相同的 rideId 对应的 Taxi Ride 事件
            // 先要看下 rideId 对应的 Taxi Fare 是否已经存在状态中
            TaxiFare fare = fareValueState.value();
            if (fare != null)// 说明对应的 rideId 的 taxi fare 事件已经到达
            {
                fareValueState.clear();
                // 输出相同的 rideId 对应的 ride 和 fare 事件
                out.collect(Tuple2.of(taxiRide, fare));
            } else {
                // 先保存 ride 事件
                rideValueState.update(taxiRide);
            }
        }

        @Override
        public void flatMap2(TaxiFare taxiFare,
                             Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // 这里是处理相同的 rideId 对应的 Taxi Fare 事件
            // 先要看下 rideId 对应的 Taxi Ride 是否已经存在状态中
            TaxiRide ride = rideValueState.value();
            if (ride != null) {
                rideValueState.clear();
                out.collect(Tuple2.of(ride, taxiFare));
            } else {
                fareValueState.update(taxiFare);
            }
        }
    }
}
