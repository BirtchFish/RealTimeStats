package com.focusmedia.newgenerate;

import java.io.Serializable;

public class ScreenPlayVo implements Serializable {

    //d.devid, d.inner_sn, d.cityname, d.pid, d.st, d.et,d.sv, d.pi, d.idx," +
    //                " d.adcustomer,d.adproduct,d.adcontent,d.ad_content_id,d.building_name,d.building_no,d.suit_kind,d.row_key,uploadcount
    public String devid;
    public String inner_sn;
    public String cityname;
    public String pid;
    public String st;
    public String et;
    public String sv;
    public String pi;
    public String idx;
    public String adcustomer;
    public String adproduct;
    public String adcontent;
    public String ad_content_id;
    public String buildingname;
    public String builingno;
    public String suit_kind;
    public long stlong;
    public long etlong;
    public String row_key;
    public String uploadcount;
    public String ad_length;


    public ScreenPlayVo() {
    }

//k.devid,k.iccid,k.inner_sn,k.pid,k.st,k.et,k.buildinglist,k.pi,k.idx,k.adcontent,k.uploadcount,suit_kind,ad_length
    public ScreenPlayVo(String devid,String iccid,String inner_sn,
                        String pid,String st,String et,String buildinglist,
                        String pi,String idx,String adcontent,String uploadcount,
                        String suit_kind,String city_name,String ad_length,long stlong,long etlong,
                        String adcustomer,String adproduct,String ad_content_id,String builingno,String buildingname){

        this.devid = devid;
        this.inner_sn = inner_sn;
        this.pid = pid;
        this.st = st;
        this.et = et;
        this.sv = buildinglist;
        this.pi = pi;
        this.idx = idx;
        this.adcontent = adcontent;
        this.uploadcount = uploadcount;
        this.suit_kind = suit_kind;
        this.ad_length  = ad_length;
        this.stlong = stlong;
        this.etlong = etlong;
        this.ad_content_id = ad_content_id;
        this.adcustomer = adcustomer;
        this.adproduct = adproduct;
        this.cityname = city_name;
        this.builingno = builingno;
        this.buildingname = buildingname;
    }

    public ScreenPlayVo(String devid, String inner_sn, String cityname, String pid,
                        String st, String et, String sv, String pi, String idx,
                        String adcustomer, String adproduct, String adcontent, String ad_content_id,
                        String buildingname, String builingno, String suit_kind,
                        long stlong, long etlong,String row_key,String uploadcount,String ad_length) {
        this.devid = devid;
        this.inner_sn = inner_sn;
        this.cityname = cityname;
        this.pid = pid;
        this.st = st;
        this.et = et;
        this.sv = sv;
        this.pi = pi;
        this.idx = idx;
        this.adcustomer = adcustomer;
        this.adproduct = adproduct;
        this.adcontent = adcontent;
        this.ad_content_id = ad_content_id;
        this.buildingname = buildingname;
        this.builingno = builingno;
        this.suit_kind = suit_kind;
        this.stlong = stlong;
        this.etlong = etlong;
        this.row_key = row_key;
        this.uploadcount = uploadcount;
        this.ad_length = ad_length;
    }


    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public String getInner_sn() {
        return inner_sn;
    }

    public void setInner_sn(String inner_sn) {
        this.inner_sn = inner_sn;
    }

    public String getCityname() {
        return cityname;
    }

    public void setCityname(String cityname) {
        this.cityname = cityname;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getSt() {
        return st;
    }

    public void setSt(String st) {
        this.st = st;
    }

    public String getEt() {
        return et;
    }

    public void setEt(String et) {
        this.et = et;
    }

    public String getSv() {
        return sv;
    }

    public void setSv(String sv) {
        this.sv = sv;
    }

    public String getPi() {
        return pi;
    }

    public void setPi(String pi) {
        this.pi = pi;
    }

    public String getIdx() {
        return idx;
    }

    public void setIdx(String idx) {
        this.idx = idx;
    }

    public String getAdcustomer() {
        return adcustomer;
    }

    public void setAdcustomer(String adcustomer) {
        this.adcustomer = adcustomer;
    }

    public String getAdproduct() {
        return adproduct;
    }

    public void setAdproduct(String adproduct) {
        this.adproduct = adproduct;
    }

    public String getAdcontent() {
        return adcontent;
    }

    public void setAdcontent(String adcontent) {
        this.adcontent = adcontent;
    }

    public String getAd_content_id() {
        return ad_content_id;
    }

    public void setAd_content_id(String ad_content_id) {
        this.ad_content_id = ad_content_id;
    }

    public String getBuildingname() {
        return buildingname;
    }

    public void setBuildingname(String buildingname) {
        this.buildingname = buildingname;
    }

    public String getBuilingno() {
        return builingno;
    }

    public void setBuilingno(String builingno) {
        this.builingno = builingno;
    }

    public String getSuit_kind() {
        return suit_kind;
    }

    public void setSuit_kind(String suit_kind) {
        this.suit_kind = suit_kind;
    }

    public long getStlong() {
        return stlong;
    }

    public void setStlong(long stlong) {
        this.stlong = stlong;
    }

    public long getEtlong() {
        return etlong;
    }

    public String getUploadcount() {
        return uploadcount;
    }

    public void setUploadcount(String uploadcount) {
        this.uploadcount = uploadcount;
    }

    public void setEtlong(long etlong) {
        this.etlong = etlong;
    }

    public String getRow_key() {
        return row_key;
    }

    public void setRow_key(String row_key) {
        this.row_key = row_key;
    }


    public String getAd_length() {
        return ad_length;
    }

    public void setAd_length(String ad_length) {
        this.ad_length = ad_length;
    }

    @Override
    public String toString() {
        return "ScreenPlayVo{" +
                "devid='" + devid + '\'' +
                ", inner_sn='" + inner_sn + '\'' +
                ", cityname='" + cityname + '\'' +
                ", pid='" + pid + '\'' +
                ", st='" + st + '\'' +
                ", et='" + et + '\'' +
                ", sv='" + sv + '\'' +
                ", pi='" + pi + '\'' +
                ", idx='" + idx + '\'' +
                ", adcustomer='" + adcustomer + '\'' +
                ", adproduct='" + adproduct + '\'' +
                ", adcontent='" + adcontent + '\'' +
                ", ad_content_id='" + ad_content_id + '\'' +
                ", buildingname='" + buildingname + '\'' +
                ", builingno='" + builingno + '\'' +
                ", suit_kind='" + suit_kind + '\'' +
                ", stlong=" + stlong +
                ", etlong=" + etlong +
                ", row_key='" + row_key + '\'' +
                ", uploadcount='" + uploadcount + '\'' +
                ", ad_length='" + ad_length + '\'' +
                '}';
    }
}
