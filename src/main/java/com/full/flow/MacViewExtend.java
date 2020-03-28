package com.full.flow;

import java.io.Serializable;

public class MacViewExtend implements Serializable {

    //citycode,city_name,mac,scan_time,sch_version,uploadcount,pls_index,play_index,plninfo,ad_content_id
    public String citycode;
    public String city_name;
    public String mac;
    public String scan_time;
    public String sch_version;
    public String uploadcount;
    public String pls_index;
    public String play_index;
    public String plninfo;
    public String ad_content_id;
    public String time;
    public String adcontent;    //代表该广告的广告描述
    public String playnums; //代表该广告的一天播放次数

    public MacViewExtend(){};

    public MacViewExtend(String citycode, String city_name, String mac, String scan_time, String sch_version, String uploadcount, String pls_index, String play_index, String plninfo, String ad_content_id, String time,String adcontent, String playnums) {
        this.citycode = citycode;
        this.city_name = city_name;
        this.mac = mac;
        this.scan_time = scan_time;
        this.sch_version = sch_version;
        this.uploadcount = uploadcount;
        this.pls_index = pls_index;
        this.play_index = play_index;
        this.plninfo = plninfo;
        this.ad_content_id = ad_content_id;
        this.time=time;
        this.adcontent = adcontent;
        this.playnums = playnums;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
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

    public String getSch_version() {
        return sch_version;
    }

    public void setSch_version(String sch_version) {
        this.sch_version = sch_version;
    }

    public String getUploadcount() {
        return uploadcount;
    }

    public void setUploadcount(String uploadcount) {
        this.uploadcount = uploadcount;
    }

    public String getPls_index() {
        return pls_index;
    }

    public void setPls_index(String pls_index) {
        this.pls_index = pls_index;
    }

    public String getPlay_index() {
        return play_index;
    }

    public void setPlay_index(String play_index) {
        this.play_index = play_index;
    }

    public String getPlninfo() {
        return plninfo;
    }

    public void setPlninfo(String plninfo) {
        this.plninfo = plninfo;
    }

    public String getAd_content_id() {
        return ad_content_id;
    }

    public void setAd_content_id(String ad_content_id) {
        this.ad_content_id = ad_content_id;
    }

    public String getAdcontent() {
        return adcontent;
    }

    public void setAdcontent(String adcontent) {
        this.adcontent = adcontent;
    }

    public String getPlaynums() {
        return playnums;
    }

    public void setPlaynums(String playnums) {
        this.playnums = playnums;
    }
}
