package com.full.flow;

import java.io.Serializable;

public class CityExposeTime implements Serializable {
    public String city_name;
    public long expose_time;    //代表曝光次数
    public long  mactimes;       //代表是再该次数下 所有的到达人次
    public String tdate;

    public CityExposeTime(){}

    public CityExposeTime(String city_name,long expose_time, long mactimes, String tdate) {
        this.city_name=city_name;
        this.expose_time = expose_time;
        this.mactimes = mactimes;
        this.tdate=tdate;
    }

    public long getExpose_time() {
        return expose_time;
    }

    public void setExpose_time(long expose_time) {
        this.expose_time = expose_time;
    }

    public long getMactimes() {
        return mactimes;
    }

    public void setMactimes(long mactimes) {
        this.mactimes = mactimes;
    }

    public String getTdate() {
        return tdate;
    }

    public void setTdate(String tdate) {
        this.tdate = tdate;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }
}
