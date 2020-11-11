package com.flink.project;

import com.flink.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 生成watermark：每次接收到一个事件的话就会生成一个 watermark
 *
 * @author tang
 */
public class ConnectedCarAssigner
        implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(ConnectedCarEvent connectedCarEvent,
                                              long extractTimestamp) {
        // 设置 watermark 值为当前事件的 event time 减去 30 秒
        // 需要注意，watermark 在以下两种情况下是不会变化的：
        // 1. 返回 null
        // 2. 当前返回的 watermark 值比上一次返回的 watermark 值还要小的时候
        return new Watermark(extractTimestamp - 30000);
    }

    @Override
    public long extractTimestamp(ConnectedCarEvent connectedCarEvent,
                                 long previousElementTimestamp) {
        return connectedCarEvent.getTimestamp();
    }
}
