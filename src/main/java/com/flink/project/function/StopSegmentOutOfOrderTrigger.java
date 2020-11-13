package com.flink.project.function;

import com.flink.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class StopSegmentOutOfOrderTrigger
        extends Trigger<ConnectedCarEvent, GlobalWindow> {

    @Override
    public TriggerResult onElement(ConnectedCarEvent element,
                                   long timestamp,
                                   GlobalWindow window,
                                   Trigger.TriggerContext ctx) throws Exception {
        if (element.getSpeed() == 0.0) {
            // 如果当前的 speed 等于 0 的话，还需要等待一下
            // 等 watermark 值等于当前元素的 event time 就触发 window 的计算
            ctx.registerEventTimeTimer(element.getTimestamp());
        }
        // 继续
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time,
                                          GlobalWindow window, TriggerContext ctx) throws Exception {
        // 继续
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time,
                                     GlobalWindow window, TriggerContext ctx) throws Exception {
        // 触发计算
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(GlobalWindow window, Trigger.TriggerContext ctx) throws Exception {

    }
}

