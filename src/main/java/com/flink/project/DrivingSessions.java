package com.flink.project;

import com.flink.datatypes.ConnectedCarEvent;
import com.flink.datatypes.GapSegment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 从车的起始开始到 过了 15 秒都没有发送事件的时候 ， 发出的所有的事件
 * @author tang
 */
public class DrivingSessions {
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
                .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
                .process(new ProcessWindowFunction<ConnectedCarEvent, GapSegment, String, TimeWindow>() {
                    @Override
                    public void process(String carId,
                                        Context context,
                                        Iterable<ConnectedCarEvent> elements,
                                        Collector<GapSegment> out) throws Exception {
                        GapSegment segment = new GapSegment(elements);
                        out.collect(segment);
                    }
                }).print();

        env.execute("DrivingSessions");
    }

}
