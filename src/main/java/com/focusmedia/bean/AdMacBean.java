package com.focusmedia.bean;

import java.io.Serializable;

public class AdMacBean implements Serializable {

    public String city_name;
    public String devid;
    public String mac;
    public String ad_content_id;
    public String ad_content;
    public String city_dev_mac_adcontent;
    public String tdate;


    public AdMacBean(){};


    public AdMacBean(String city_name, String devid, String mac, String ad_content_id, String ad_content,String tdate) {
        this.city_name = city_name;
        this.devid = devid;
        this.mac = mac;
        this.ad_content_id = ad_content_id;
        this.ad_content = ad_content;
        this.city_dev_mac_adcontent=this.city_name+","+this.devid+","+this.mac+","+this.ad_content;
        this.tdate=tdate;
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

    public String getAd_content_id() {
        return ad_content_id;
    }

    public void setAd_content_id(String ad_content_id) {
        this.ad_content_id = ad_content_id;
    }

    public String getAd_content() {
        return ad_content;
    }

    public void setAd_content(String ad_content) {
        this.ad_content = ad_content;
    }

    public String getCity_dev_mac_adcontent() {
        this.city_dev_mac_adcontent=this.city_name+","+this.devid+","+this.mac+","+this.ad_content;
        return city_dev_mac_adcontent;
    }

    public void setCity_dev_mac_adcontent(String city_dev_mac_adcontent) {
        this.city_dev_mac_adcontent = city_dev_mac_adcontent;
    }

    public String getTdate() {
        return tdate;
    }

    public void setTdate(String tdate) {
        this.tdate = tdate;
    }

    @Override
    public String toString() {
        return "AdMacBean{" +
                "city_name='" + city_name + '\'' +
                ", devid='" + devid + '\'' +
                ", mac='" + mac + '\'' +
                ", ad_content_id='" + ad_content_id + '\'' +
                ", ad_content='" + ad_content + '\'' +
                '}';
    }
}
