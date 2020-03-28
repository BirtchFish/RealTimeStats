package com.focusmedia.bean;

import java.io.Serializable;

public class MacRowCity implements Serializable {
    public String city_name;
    public String devid;
    public String mac;
    public String scan_time;


    public MacRowCity(){}

    public MacRowCity(String city_name,String devid, String mac, String scan_time) {
        this.city_name=city_name;
        this.devid = devid;
        this.mac = mac;
        this.scan_time = scan_time;
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

    public String getScan_time() {
        return scan_time;
    }

    public void setScan_time(String scan_time) {
        this.scan_time = scan_time;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }
}


