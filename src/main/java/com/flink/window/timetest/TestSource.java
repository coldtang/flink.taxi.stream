package com.flink.window.timetest;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 模拟消息发送
 * @author tang
 * */
public class TestSource implements SourceFunction<String> {

    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 控制大约在 10 秒的倍数的时间点发送事件
        String currTime = String.valueOf(System.currentTimeMillis());
        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }
        System.out.println("开始发送事件的时间：" + dateFormat.format(System.currentTimeMillis()));
        // 第 13 秒发送两个事件
        TimeUnit.SECONDS.sleep(13);
        ctx.collect("a," + System.currentTimeMillis());
        // 产生了一个事件，但是由于网络原因，事件没有发送
        String event = "a," + System.currentTimeMillis();
        // 第 16 秒发送一个事件
        TimeUnit.SECONDS.sleep(3);
        ctx.collect("a," + System.currentTimeMillis());
        // 第 19 秒的时候发送
        TimeUnit.SECONDS.sleep(3);
        ctx.collect(event);

        TimeUnit.SECONDS.sleep(300);
    }

    @Override
    public void cancel() {

    }
}
