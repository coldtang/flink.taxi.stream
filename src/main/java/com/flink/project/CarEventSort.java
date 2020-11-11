package com.flink.project;

import com.flink.datatypes.ConnectedCarEvent;
import com.flink.project.function.EventSortFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.Env;

/**
 * 实时对无序的 Car Event 中的每一辆车所有的事件按照时间升序排列
 * 1. 需要读取数据源，并将字符串转成 ConnectedCarEvent
 * 2. 按照 carId 分组，然后对每个 carId 所有的事件按照 event time 升序排列
 *
 * @author tang
 */
public class CarEventSort {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.readTextFile("data\\car\\carOutOfOrder.csv");

        DataStream<ConnectedCarEvent> carEnvent = source.map(m -> ConnectedCarEvent.fromString(m))
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());

        // 升序排序
        carEnvent.keyBy(ConnectedCarEvent::getCarId).process(new EventSortFunction()).print();

        env.execute("CarEventSort");
    }
}
