package com.focusmedia.bean;

import java.io.Serializable;

public class CityAdConExpMacNUm implements Serializable {

    public String city_name;
    public String ad_content;
    public int expose_time;
    public int macnums;

    public CityAdConExpMacNUm(){}

    public CityAdConExpMacNUm(String city_name, String ad_content, int expose_time, int macnums) {
        this.city_name = city_name;
        this.ad_content = ad_content;
        this.expose_time = expose_time;
        this.macnums = macnums;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getAd_content() {
        return ad_content;
    }

    public void setAd_content(String ad_content) {
        this.ad_content = ad_content;
    }

    public int getExpose_time() {
        return expose_time;
    }

    public void setExpose_time(int expose_time) {
        this.expose_time = expose_time;
    }

    public int getMacnums() {
        return macnums;
    }

    public void setMacnums(int macnums) {
        this.macnums = macnums;
    }
}
