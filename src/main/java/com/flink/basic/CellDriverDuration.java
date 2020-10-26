package com.flink.basic;

import com.flink.DataFilePath;
import com.flink.datatypes.EnrichedTaxiRide;
import com.flink.source.GzpFileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;

/**
 * 实时计算出从每一个单元格启动的车开了最长的时间
 *
 * @author tang
 */
public class CellDriverDuration implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStreamSource<String> source = env.addSource(new GzpFileSource(TAXI_RIDE_PATH));

        // 数据处理
        DataStream<EnrichedTaxiRide> rides = source
                .flatMap(new RideEnrichingWithFlatMap.EnrichedTaxiRideFlatMap());

        //计算时间
        DataStream<Tuple2<Integer, Integer>> minsCellDriver = rides
                .flatMap(new FlatMapFunction<EnrichedTaxiRide, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(EnrichedTaxiRide ride,
                                        Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        if (!ride.isStart()) {
                            int startCell = ride.getStartCell();
                            Interval rideInterval = new Interval(ride.getStartTime(), ride.getEndTime());
                            Integer time = rideInterval.toDuration().toStandardMinutes().getMinutes();
                            out.collect(Tuple2.of(startCell, time));
                        }
                    }
                });

        //数据打印
        minsCellDriver.keyBy(0).maxBy(1).print();

        env.execute("flink CellDriverDuration");
    }
}
