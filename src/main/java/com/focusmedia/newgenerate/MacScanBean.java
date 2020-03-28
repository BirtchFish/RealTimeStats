package com.focusmedia.newgenerate;

import java.io.Serializable;

public class MacScanBean implements Serializable {

    //devid,iccid,inner_sn,mac,sch_version,uploadcount,pls_index,play_index,plninfo, scan_time,sscan_time_long
    public String devid;
    public String iccid;
    public String inner_sn;
    public String mac;
    public String sch_version;
    public String uploadcount;
    public String pls_index;
    public String play_index;
    public String plninfo;
    public String scan_time;
    public Long scan_time_long;
    public Long time_diff;

    public MacScanBean(){}

    public MacScanBean(String devid, String iccid, String inner_sn, String mac, String sch_version,
                       String uploadcount, String pls_index, String play_index, String plninfo, String scan_time,
                       long scan_time_long,long time_diff) {
        this.devid = devid;
        this.iccid = iccid;
        this.inner_sn = inner_sn;
        this.mac = mac;
        this.sch_version = sch_version;
        this.uploadcount = uploadcount;
        this.pls_index = pls_index;
        this.play_index = play_index;
        this.plninfo = plninfo;
        this.scan_time = scan_time;
        this.scan_time_long=scan_time_long;
        this.time_diff=time_diff;
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

    public String getScan_time() {
        return scan_time;
    }

    public void setScan_time(String scan_time) {
        this.scan_time = scan_time;
    }

    public Long getScan_time_long() {
        return scan_time_long;
    }

    public void setScan_time_long(Long scan_time_long) {
        this.scan_time_long = scan_time_long;
    }


    public Long getTime_diff() {
        return time_diff;
    }

    public void setTime_diff(Long time_diff) {
        this.time_diff = time_diff;
    }

    @Override
    public String toString() {
        return "MacScanBean{" +
                "devid='" + devid + '\'' +
                ", iccid='" + iccid + '\'' +
                ", inner_sn='" + inner_sn + '\'' +
                ", mac='" + mac + '\'' +
                ", sch_version='" + sch_version + '\'' +
                ", uploadcount='" + uploadcount + '\'' +
                ", pls_index='" + pls_index + '\'' +
                ", play_index='" + play_index + '\'' +
                ", plninfo='" + plninfo + '\'' +
                ", scan_time='" + scan_time + '\'' +
                ", scan_time_long=" + scan_time_long +
                ", time_diff=" + time_diff +
                '}';
    }
}
