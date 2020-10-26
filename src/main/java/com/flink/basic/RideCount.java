package com.flink.basic;

import com.flink.DataFilePath;
import com.flink.datatypes.EnrichedTaxiRide;
import com.flink.datatypes.TaxiRide;
import com.flink.source.GzpFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 计算每一个司机开了多少次车
 * @author tang
 */
public class RideCount implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStreamSource<String> source = env.addSource(new GzpFileSource(TAXI_RIDE_PATH));

        // 数据处理
        // 格式化
        DataStream<TaxiRide> rides = source.map(line -> TaxiRide.fromString(line))
                .filter(new RideCleansing.IsInNYCByLatLon());
        //转换数据格式
        DataStream<Tuple2<Long,Long>> driverInfo=rides.map(ride->Tuple2.of(ride.getDriverId(),1L))
                .returns(Types.TUPLE(Types.LONG,Types.LONG));
        // 计算总次数
        DataStream<Tuple2<Long,Long>> countInfo= driverInfo.keyBy(0)
                .sum(1);

        // 数据打印
        countInfo.print();

        env.execute("flink RideCount");
    }
}
