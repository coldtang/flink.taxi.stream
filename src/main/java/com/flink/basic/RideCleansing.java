package com.flink.basic;


import com.flink.DataFilePath;
import com.flink.datatypes.TaxiRide;
import com.flink.source.GzpFileSource;
import com.flink.util.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.print.DocFlavor;

/**
 * 过滤起始位置和终点位置都在纽约地理范围内的时间
 *
 * @author tang
 */
public class RideCleansing implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStreamSource<String> source = env.addSource(new GzpFileSource(TAXI_RIDE_PATH));

        // 数据处理
        // 格式化
        DataStream<TaxiRide> rides = source.map(new MapFunction<String, TaxiRide>() {
            @Override
            public TaxiRide map(String line) throws Exception {
                return TaxiRide.fromString(line);
            }
        });
        // 过滤
        DataStream<TaxiRide> filter = rides.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxiRide) throws Exception {
                return GeoUtils.isInNYC(taxiRide.getStartLon(), taxiRide.getStartLat()) &&
                        GeoUtils.isInNYC(taxiRide.getEndLon(), taxiRide.getEndLat());
            }
        });
        // 数据打印
        filter.print();

        env.execute("flink RideCleansing");
    }

}
