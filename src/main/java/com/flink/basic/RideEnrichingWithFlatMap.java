package com.flink.basic;

import com.flink.DataFilePath;
import com.flink.datatypes.EnrichedTaxiRide;
import com.flink.datatypes.TaxiRide;
import com.flink.source.GzpFileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 过滤起始位置和终点位置都在纽约地理范围内的时间
 * 计算经纬度网格id
 * @author tang
 */
public class RideEnrichingWithFlatMap implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStreamSource<String> source = env.addSource(new GzpFileSource(TAXI_RIDE_PATH));

        // 数据处理
        DataStream<EnrichedTaxiRide> rides = source.flatMap(new EnrichedTaxiRideFlatMap());

        //数据打印
        rides.print();

        env.execute("flink RideEnrichingWithFlatMap");
    }

    /**
     * 计算网格id
     */
    public static  class  EnrichedTaxiRideFlatMap implements FlatMapFunction<String, EnrichedTaxiRide>{
        @Override
        public void flatMap(String line, Collector<EnrichedTaxiRide> out) throws Exception {
            // 格式化
            TaxiRide ride =TaxiRide.fromString(line);
            // 过滤
            RideCleansing.IsInNYCByLatLon filter=new RideCleansing.IsInNYCByLatLon();

            if(filter.filter(ride))
            {
                out.collect(new EnrichedTaxiRide(ride));
            }
        }
    }
}
