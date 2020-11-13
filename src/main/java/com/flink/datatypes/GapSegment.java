package com.flink.datatypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 从车的起始开始到 过了 15 秒都没有发送事件的时候 ， 发出的所有的事件
 * @author tang
 */
public class GapSegment extends Segment {
    public GapSegment(Iterable<ConnectedCarEvent> events) {
        List<ConnectedCarEvent> list = new ArrayList<>();
        for (Iterator<ConnectedCarEvent> iterator = events.iterator(); iterator.hasNext();) {
            ConnectedCarEvent event = iterator.next();
            list.add(event);
        }

        this.setLength(list.size());

        if (this.getLength() > 0) {
            this.setCarId(list.get(0).getCarId());
            this.setStartTime(Segment.minTimestamp(list));
            this.setMaxSpeed(Segment.maxSpeed(list));
            this.setErraticness(Segment.stddevThrottle(list));
        }
    }
}