package com.xiongtian.apitest.beans;

/**
 * Created by xiongtian on 2022/2/10 10:54
 */
// 传感器温度读数的数据类型
public class SensorReading {

    // 属性：id,时间戳，温度值
    private String id;

    private Long timeStamp;

    private Double temperature;


    public SensorReading() {
    }

    public SensorReading(String id, Long timeStamp, Double temperature) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timeStamp=" + timeStamp +
                ", temperature=" + temperature +
                '}';
    }
}
