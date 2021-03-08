# flink.taxi.stream
## 技术栈：java、flink、kafka、hdfs
* 五种flink状态实践：ValueState、MapState、ListState、ReducingState、AggregatingState
* 利用flink窗口处理有序数据、无序数据
* 利用StateBackend、checkpoint提高flink的容错性
## 实时统计分析每一个出租车的动态
* 模拟数据发送，利用获取的数据，基于eventTime、watermark处理实时数据。统计分析每个司机收入、停止次数等信息。
