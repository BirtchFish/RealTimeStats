package com.focusmedia.newgenerate;

import java.io.Serializable;

public class MacScanVoAD implements Serializable {

    public String devid;
    public String iccid;
    public String inner_sn;
    public String mac;
    public String plninfo;
    public String sch_version;
    public String uploadcount;
    public String pls_index;
    public String play_index;
    public String scan_time;
    public String signal;
    public long scantimelong;
    public String adcustomer;
    public String adproduct;
    public String adcontent;
    public String ad_content_id;
    public String buildingname;
    public String  buildingno;
    public long time_diff;
    public String peopleid;
    public String suit_kind;
    public String ad_length;
    public String city_name;

    public MacScanVoAD() {
    }



    public MacScanVoAD(String devid, String iccid, String inner_sn, String mac,
                       String plninfo, String sch_version, String uploadcount,
                       String pls_index, String play_index, String scan_time,
                       String signal, long scantimelong, String adcustomer,
                       String adproduct, String adcontent, String ad_content_id,
                       String buildingname, String buildingno, long time_diff, String peopleid, String suit_kind, String ad_length) {
        this.devid = devid;
        this.iccid = iccid;
        this.inner_sn = inner_sn;
        this.mac = mac;
        this.plninfo = plninfo;
        this.sch_version = sch_version;
        this.uploadcount = uploadcount;
        this.pls_index = pls_index;
        this.play_index = play_index;
        this.scan_time = scan_time;
        this.signal = signal;
        this.scantimelong = scantimelong;
        this.adcustomer = adcustomer;
        this.adproduct = adproduct;
        this.adcontent = adcontent;
        this.ad_content_id = ad_content_id;
        this.buildingname = buildingname;
        this.buildingno = buildingno;
        this.time_diff = time_diff;
        this.peopleid = peopleid;
        this.suit_kind=suit_kind;
        this.ad_length = ad_length;
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

    public String getPlninfo() {
        return plninfo;
    }

    public void setPlninfo(String plninfo) {
        this.plninfo = plninfo;
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

    public String getScan_time() {
        return scan_time;
    }

    public void setScan_time(String scan_time) {
        this.scan_time = scan_time;
    }

    public String getSignal() {
        return signal;
    }

    public void setSignal(String signal) {
        this.signal = signal;
    }


    public long getScantimelong() {
        return scantimelong;
    }

    public void setScantimelong(long scantimelong) {
        this.scantimelong = scantimelong;
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

    public long getTime_diff() {
        return time_diff;
    }

    public void setTime_diff(long time_diff) {
        this.time_diff = time_diff;
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


    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    @Override
    public String toString() {
        return "MacScanVoAD{" +
                "devid='" + devid + '\'' +
                ", iccid='" + iccid + '\'' +
                ", inner_sn='" + inner_sn + '\'' +
                ", mac='" + mac + '\'' +
                ", plninfo='" + plninfo + '\'' +
                ", sch_version='" + sch_version + '\'' +
                ", uploadcount='" + uploadcount + '\'' +
                ", pls_index='" + pls_index + '\'' +
                ", play_index='" + play_index + '\'' +
                ", scan_time='" + scan_time + '\'' +
                ", signal='" + signal + '\'' +
                ", scantimelong=" + scantimelong +
                ", adcustomer='" + adcustomer + '\'' +
                ", adproduct='" + adproduct + '\'' +
                ", adcontent='" + adcontent + '\'' +
                ", ad_content_id='" + ad_content_id + '\'' +
                ", buildingname='" + buildingname + '\'' +
                ", buildingno='" + buildingno + '\'' +
                ", time_diff=" + time_diff +
                ", peopleid='" + peopleid + '\'' +
                ", suit_kind='" + suit_kind + '\'' +
                ", ad_length='" + ad_length + '\'' +
                ", city_name='" + city_name + '\'' +
                '}';
    }
}
