package com.flink.state.function;

import com.flink.DataFilePath;
import com.flink.datatypes.TaxiFare;
import com.flink.datatypes.TaxiRide;
import com.flink.source.GzpFileSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(100 * 1024 * 1024);
        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://master:9001/checkpoint-path/");
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://master:9001/checkpoint-path/");

        env.setStateBackend(rocksDBStateBackend);

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
