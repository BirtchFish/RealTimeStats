package com.focusmedia.bean;

import java.io.Serializable;

public class CityAdGrpBean implements Serializable {

    public String city_name;
    public String adcontent;
    public int expose_time;
    public int mactimes;
    public String tdate;

    public CityAdGrpBean(){}

    public CityAdGrpBean(String city_name, String adcontent, int expose_time, int mactimes,String tdate) {
        this.city_name = city_name;
        this.adcontent = adcontent;
        this.expose_time = expose_time;
        this.mactimes = mactimes;
        this.tdate=tdate;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getAdcontent() {
        return adcontent;
    }

    public void setAdcontent(String adcontent) {
        this.adcontent = adcontent;
    }

    public int getExpose_time() {
        return expose_time;
    }

    public void setExpose_time(int expose_time) {
        this.expose_time = expose_time;
    }

    public int getMactimes() {
        return mactimes;
    }

    public void setMactimes(int mactimes) {
        this.mactimes = mactimes;
    }

    public String getTdate() {
        return tdate;
    }

    public void setTdate(String tdate) {
        this.tdate = tdate;
    }
}
