package com.focusmedia.newgenerate;

import com.focusmedia.util.DevDateUtil;

import java.io.Serializable;

public class MacResultBean implements Serializable {

    public String devid;
    public String iccid;
    public String inner_sn;
    public String mac;
    public String sch_version;
    public String uploadcount;
    public String pls_index;
    public String play_index;
    public String plninfo;
    public String start_time;
    public long start_time_long;
    public String end_time;
    public long end_time_long;
    public long time_diff;
    public String adcustomer;
    public String adproduct;
    public String adcontent;
    public String ad_content_id;
    public String buildingname;
    public String  buildingno;
    public String peopleid;
    public String suit_kind;
    public String ad_length;

    public MacResultBean(){}

    public MacResultBean(String devid, String iccid, String inner_sn, String mac, String sch_version, String uploadcount,
                         String pls_index, String play_index, String plninfo, String start_time, long start_time_long,
                         String end_time, long end_time_long, long time_diff,String adcustomer,String adproduct,
                         String adcontent,String ad_content_id,String buildingname,String buildingno,String peopleid,
                         String suit_kind,String ad_length) {
        this.devid = devid;
        this.iccid = iccid;
        this.inner_sn = inner_sn;
        this.mac = mac;
        this.sch_version = sch_version;
        this.uploadcount = uploadcount;
        this.pls_index = pls_index;
        this.play_index = play_index;
        this.plninfo = plninfo;
        this.start_time = start_time;
        this.start_time_long = start_time_long;
        this.end_time = end_time;
        this.end_time_long = end_time_long;
        this.time_diff=time_diff;
        this.adcustomer = adcustomer;
        this.adproduct = adproduct;
        this.adcontent = adcontent;
        this.ad_content_id = ad_content_id;
        this.buildingname = buildingname;
        this.buildingno = buildingno;
        this.peopleid = peopleid;
        this.suit_kind = suit_kind;
        this.ad_length  = ad_length;
    }

    //直接
    public void updateEndTime(long next_scan_time_long) {
        this.end_time_long=next_scan_time_long;
        this.end_time= DevDateUtil.longTimeToStr(end_time_long);
    }

    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public String getIccid() {
        return iccid;
    }

    public void setIccid(String iccid) {
        this.iccid = iccid;
    }

    public String getInner_sn() {
        return inner_sn;
    }

    public void setInner_sn(String inner_sn) {
        this.inner_sn = inner_sn;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
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

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public long getStart_time_long() {
        return start_time_long;
    }

    public void setStart_time_long(long start_time_long) {
        this.start_time_long = start_time_long;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public long getEnd_time_long() {
        return end_time_long;
    }

    public void setEnd_time_long(long end_time_long) {
        this.end_time_long = end_time_long;
    }

    public long getTime_diff() {
        return time_diff;
    }

    public void setTime_diff(long time_diff) {
        this.time_diff = time_diff;
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

    public String getBuildingno() {
        return buildingno;
    }

    public void setBuildingno(String buildingno) {
        this.buildingno = buildingno;
    }

    public String getPeopleid() {
        return peopleid;
    }

    public void setPeopleid(String peopleid) {
        this.peopleid = peopleid;
    }

    public String getSuit_kind() {
        return suit_kind;
    }

    public void setSuit_kind(String suit_kind) {
        this.suit_kind = suit_kind;
    }

    public String getAd_length() {
        return ad_length;
    }

    public void setAd_length(String ad_length) {
        this.ad_length = ad_length;
    }

    @Override
    public String toString() {
        return "MacResultBean{" +
                "devid='" + devid + '\'' +
                ", iccid='" + iccid + '\'' +
                ", inner_sn='" + inner_sn + '\'' +
                ", mac='" + mac + '\'' +
                ", sch_version='" + sch_version + '\'' +
                ", uploadcount='" + uploadcount + '\'' +
                ", pls_index='" + pls_index + '\'' +
                ", play_index='" + play_index + '\'' +
                ", plninfo='" + plninfo + '\'' +
                ", start_time='" + start_time + '\'' +
                ", start_time_long=" + start_time_long +
                ", end_time='" + end_time + '\'' +
                ", end_time_long=" + end_time_long +
                ", time_diff=" + time_diff +
                ", adcustomer='" + adcustomer + '\'' +
                ", adproduct='" + adproduct + '\'' +
                ", adcontent='" + adcontent + '\'' +
                ", ad_content_id='" + ad_content_id + '\'' +
                ", buildingname='" + buildingname + '\'' +
                ", buildingno='" + buildingno + '\'' +
                ", peopleid='" + peopleid + '\'' +
                ", suit_kind='" + suit_kind + '\'' +
                ", ad_length='" + ad_length + '\'' +
                '}';
    }
}
