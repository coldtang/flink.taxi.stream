package com.flink.state.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

/**
 *  MapState<K, V> ：这个状态为每一个 key 保存一个 Map 集合
 *  put() 将对应的 key 的键值对放到状态中
 *  values() 拿到 MapState 中所有的 value
 *  clear() 清除状态
 * @author tang
 */
public class CountWindowAverageWithMapState
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    /**
     * managed keyed state
     * MapState ：key 是一个唯一的值，value 是接收到的相同的 key 对应的 value 的值
     */
    private MapState<String,Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册状态
        MapStateDescriptor<String,Long> descriptor =
                new MapStateDescriptor<String, Long>(
                        "average",  // 状态的名字
                        String.class,Long.class); // 状态存储的数据类型
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out)
            throws Exception {
        mapState.put(UUID.randomUUID().toString(),element.f1);

        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        List<Long> allelement = Lists.newArrayList(mapState.values());
        if (allelement.size() >= 3) {
            int count = 0;
            long sum = 0;
            for (Long value : allelement) {
                count++;
                sum += value;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(element.f0, avg));
            //  清空状态值
            mapState.clear();
        }
    }
}
