package com.flink.state.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author tang
 */
public class ContainsValueFunction
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

    private AggregatingState<Long, String> containsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册状态
        AggregatingStateDescriptor<Long, String, String> descriptor =
                new AggregatingStateDescriptor<Long, String, String>(
                        "containsValue",  // 状态的名字
                        new AggregateFunction<Long, String, String>() {
                            @Override
                            public String createAccumulator() {
                                return "contains:";
                            }

                            @Override
                            public String add(Long aLong, String s) {
                                if ("contains:".equals(s)) {
                                    return s + aLong;
                                }
                                return s + " and " + aLong;
                            }

                            @Override
                            public String getResult(String s) {
                                return s;
                            }

                            /*
                            * TODO
                            * */
                            @Override
                            public String merge(String s, String acc1) {
                                return s +" and "+ acc1;
                            }
                        }
                        , String.class); // 状态存储的数据类型
        containsState = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, String>> out)
            throws Exception {
        // 更新状态值
        containsState.add(element.f1);

        out.collect(Tuple2.of(element.f0, containsState.get()));
    }
}