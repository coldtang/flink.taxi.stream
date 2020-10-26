package com.flink.datatypes;

import com.flink.util.GeoUtils;

/**
* 计算经纬度网格id
 * @author tang
*/
public class EnrichedTaxiRide extends TaxiRide {
    /**
    * 起始位置所属网格的 id
    */
    private int startCell;

    /**
    * 终止位置所属网格的 id
    */
    private int endCell;

    public EnrichedTaxiRide(TaxiRide ride) {
        super.copyData(ride);
        this.startCell = GeoUtils.mapToGridCell(ride.getStartLon(), ride.getStartLat());
        this.endCell = GeoUtils.mapToGridCell(ride.getEndLon(), ride.getEndLat());
    }

    public int getStartCell() {
        return startCell;
    }

    public int getEndCell() {
        return endCell;
    }

    @Override
    public String toString() {
        return "EnrichedTaxiRide{" +
                "startCell=" + startCell +
                ", endCell=" + endCell + "," +
                super.toString() +
                '}';
    }
}
