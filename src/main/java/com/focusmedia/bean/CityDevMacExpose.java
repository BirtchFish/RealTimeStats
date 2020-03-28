package com.focusmedia.bean;

import java.io.Serializable;

public class CityDevMacExpose implements Serializable {

    public String city_name;
    public String devid;
    public String mac;
    public String adcontent;
    public int expose_times;
    public String key;

    public CityDevMacExpose(){}

    public CityDevMacExpose(String city_name, String devid, String mac, String adcontent, int expose_times) {
        this.city_name = city_name;
        this.devid = devid;
        this.mac = mac;
        this.adcontent = adcontent;
        this.expose_times = expose_times;
        this.key=this.city_name+","+this.adcontent+","+this.expose_times;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getAdcontent() {
        return adcontent;
    }

    public void setAdcontent(String adcontent) {
        this.adcontent = adcontent;
    }

    public int getExpose_times() {
        return expose_times;
    }

    public void setExpose_times(int expose_times) {
        this.expose_times = expose_times;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
