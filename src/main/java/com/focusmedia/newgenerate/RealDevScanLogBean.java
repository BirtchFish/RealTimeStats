package com.focusmedia.newgenerate;

import java.io.Serializable;

public class RealDevScanLogBean implements Serializable{

    //devid,sch_version,uploadcount,pls_index,play_index,plninfo,
    // scan_time,citycode,ad_content_id
    public String devid;
    public String sch_version;
    public String uploadcount;
    public String pls_index;
    public String play_index;
    public String plninfo;
    public String scan_time;
    public String citycode;
    public String ad_content_id;
    public long scan_time_long;
    public String seqkey;
    public String inner_sn;

    public RealDevScanLogBean(){}

    public RealDevScanLogBean(String devid, String sch_version, String uploadcount, String pls_index,
                              String play_index, String plninfo, String scan_time, String citycode,
                              String ad_content_id,long scanlong,String seqkey,String inner_sn) {
        this.devid = devid;
        this.sch_version = sch_version;
        this.uploadcount = uploadcount;
        this.pls_index = pls_index;
        this.play_index = play_index;
        this.plninfo = plninfo;
        this.scan_time = scan_time;
        this.citycode = citycode;
        this.ad_content_id = ad_content_id;
        this.scan_time_long=scanlong;
        this.seqkey=seqkey;
        this.inner_sn=inner_sn;
    }


    public String getSeqkey() {
        return seqkey;
    }

    public void setSeqkey(String seqkey) {
        this.seqkey = seqkey;
    }

    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
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

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getAd_content_id() {
        return ad_content_id;
    }

    public void setAd_content_id(String ad_content_id) {
        this.ad_content_id = ad_content_id;
    }


    public long getScan_time_long() {
        return scan_time_long;
    }

    public void setScan_time_long(long scan_time_long) {
        this.scan_time_long = scan_time_long;
    }

    public String getInner_sn() {
        return inner_sn;
    }

    public void setInner_sn(String inner_sn) {
        this.inner_sn = inner_sn;
    }

    @Override
    public String toString() {
        return "RealDevScanLogBean{" +
                "devid='" + devid + '\'' +
                ", sch_version='" + sch_version + '\'' +
                ", uploadcount='" + uploadcount + '\'' +
                ", pls_index='" + pls_index + '\'' +
                ", play_index='" + play_index + '\'' +
                ", plninfo='" + plninfo + '\'' +
                ", scan_time='" + scan_time + '\'' +
                ", citycode='" + citycode + '\'' +
                ", ad_content_id='" + ad_content_id + '\'' +
                ", scan_time_long=" + scan_time_long +
                ", seqkey='" + seqkey + '\'' +
                '}';
    }
}
