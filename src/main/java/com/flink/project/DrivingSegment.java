package com.flink.project;

import com.flink.datatypes.ConnectedCarEvent;
import com.flink.project.function.CreateStopSegment;
import com.flink.project.function.StopSegmentEvictor;
import com.flink.project.function.StopSegmentOutOfOrderTrigger;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

/**
 * 实时计算每辆车的 StopSegment
 * @author tang
 */
public class DrivingSegment {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据源
        DataStream<String> carData = env.readTextFile("data\\car\\carOutOfOrder.csv");
        // 字符串转成 ConnectedCarEvent
        DataStream<ConnectedCarEvent> events = carData
                .map((String line) -> ConnectedCarEvent.fromString(line))
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());

        // 按照 carId 进行分组
        events.keyBy(event -> event.getCarId())
                .window(GlobalWindows.create()) // 先将分组之后的数据放在全局的窗口
                .trigger(new StopSegmentOutOfOrderTrigger()) // 当 speed == 0 的事件来的时候就触发 window 的计算
                .evictor(new StopSegmentEvictor())
                .process(new CreateStopSegment())
                .print();

        env.execute("DrivingSegment");
    }
}
