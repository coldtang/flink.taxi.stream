package com.flink.state.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * ReducingState<T> ：这个状态为每一个 key 保存一个聚合之后的值
 * get() 获取状态值
 * add()  更新状态值，将数据放到状态中
 * clear() 清除状态
 *
 * @author tang
 */
public class SumWithReducingState
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * managed keyed state
     * 用于保存每一个 key 对应的 value 的总值
     */
    private ReducingState<Long> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册状态
        ReducingStateDescriptor<Long> descriptor =
                new ReducingStateDescriptor<Long>(
                        "sum",  // 状态的名字
                        new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long t1, Long t2) throws Exception {
                                return t1 + t2;
                            }
                        }, Long.class); // 状态存储的数据类型
        sumState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out)
            throws Exception {
        // 更新状态值
        sumState.add(element.f1);

        out.collect(Tuple2.of(element.f0, sumState.get()));
    }
}