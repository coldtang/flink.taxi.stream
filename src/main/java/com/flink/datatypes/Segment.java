package com.flink.datatypes;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Segment {
    private String carId;
    // 起始时间
    private Long startTime;
    // 事件的数量
    private int length;
    // 最高速度
    private float maxSpeed;
    // 不稳定程度
    private float erraticness;

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public float getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(float maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public float getErraticness() {
        return erraticness;
    }

    public void setErraticness(float erraticness) {
        this.erraticness = erraticness;
    }

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public String erraticnessDesc() {
        switch ((int) (erraticness / 2.5)) {
            case 0:
                return "calm";
            case 1:
                return "busy";
            default:
                return "crazy";
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(carId).append(",")
                .append(startTime).append(",")
                .append(length).append(" events,")
                .append(maxSpeed).append(" kph,")
                .append(erraticnessDesc());

        return sb.toString();
    }

    protected static float maxSpeed(List<ConnectedCarEvent> events) {
        ConnectedCarEvent fastest = Collections.max(events, new CompareBySpeed());
        return fastest.getSpeed();
    }

    protected static long minTimestamp(List<ConnectedCarEvent> events) {
        ConnectedCarEvent first = Collections.min(events, new CompareByTimestamp());
        return first.getTimestamp();
    }

    /**
     *  油门标准差
     * @param array
     * @return
     */
    protected static float stddevThrottle(List<ConnectedCarEvent> array) {
        float sum = 0.0f;
        float mean;
        float sum_of_sq_diffs = 0;

        for (ConnectedCarEvent event : array) {
            sum += event.getThrottle();
        }

        mean = sum / array.size();
        for (ConnectedCarEvent event : array) {
            sum_of_sq_diffs += (event.getThrottle() - mean) * (event.getThrottle() - mean);
        }

        return (float) Math.sqrt(sum_of_sq_diffs / array.size());
    }

    private static class CompareBySpeed implements Comparator<ConnectedCarEvent> {
        @Override
        public int compare(ConnectedCarEvent a, ConnectedCarEvent b) {
            return Float.compare(a.getSpeed(), b.getSpeed());
        }
    }

    private static class CompareByTimestamp implements Comparator<ConnectedCarEvent> {

        @Override
        public int compare(ConnectedCarEvent a, ConnectedCarEvent b) {
            return Long.compare(a.getTimestamp(), b.getTimestamp());
        }
    }
}
