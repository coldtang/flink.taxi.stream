package com.flink.project.function;

import com.flink.datatypes.ConnectedCarEvent;
import com.flink.datatypes.StoppedSegment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 输出StopSegment
 * @author tang
 */
public class CreateStopSegment extends ProcessWindowFunction<
        ConnectedCarEvent, StoppedSegment, String, GlobalWindow> {

    @Override
    public void process(String carId,
                        Context context,
                        Iterable<ConnectedCarEvent> events,
                        Collector<StoppedSegment> out) throws Exception {
        StoppedSegment segment = new StoppedSegment(events);
        if (segment.getLength() > 0) {
            out.collect(segment);
        }
    }
}
