package com.flink.project.function;

import com.flink.datatypes.ConnectedCarEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.PriorityQueue;

/**
 * 当每次接收到一个元素的时候，先将这个事件放到 PriorityQueue，按照 event time 进行升序排列
 * 当乱序的事件都到了，则触发定时器将排序好的事件输出
 *
 * @author tang
 */
public class EventSortFunction
        extends KeyedProcessFunction<String, ConnectedCarEvent, ConnectedCarEvent> {
    public static final OutputTag<String> outputTag = new OutputTag<String>("late") {
    };

    // 使用PriorityQueue进行排序
    private ValueState<PriorityQueue<ConnectedCarEvent>> queueValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>> descriptor = new ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>>(
                "sorted-events",
                TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>() {
                })
        );

        queueValueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ConnectedCarEvent element,
                               Context context,
                               Collector<ConnectedCarEvent> out) throws Exception {
        // 拿到当前事件的 event time
        long currentEventTime = context.timestamp();
        // 拿到当前的 watermark 值
        TimerService timerService = context.timerService();
        long currentWatermark = timerService.currentWatermark();

        if (currentEventTime > currentWatermark) {
            PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
            if (queue == null) {
                queue = new PriorityQueue<ConnectedCarEvent>();
            }
            // 将事件放到优先级队列中
            queue.add(element);
            queueValueState.update(queue);

            // 当前的 watermark 值到了当前的 event 的 event time 的时候才触发定时器
            // 相当于每一个 event 等待了 30 秒钟再输出
            timerService.registerEventTimeTimer(element.getTimestamp());
        } else {
            // 迟到事件
            context.output(outputTag, element.getId());
        }
    }

    /**
     * 定时输出数据
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<ConnectedCarEvent> out) throws Exception {
        PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
        ConnectedCarEvent head = queue.peek();
        long currentWatermark = ctx.timerService().currentWatermark();
        // 输出的条件：
        // 1. 队列不能为空
        // 2. 拿出来的事件的 event time 需要小于当前的 watermark
        while (head != null && head.getTimestamp() <= currentWatermark) {
            out.collect(head);
            queue.remove(head);
            head = queue.peek();
        }
    }
}
