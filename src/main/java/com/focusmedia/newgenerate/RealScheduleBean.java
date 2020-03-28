package com.focusmedia.newgenerate;

import java.io.Serializable;

public class RealScheduleBean implements Serializable{
    //buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno
    //  nplinnerseqno,uploadcount,adlength
    public String buildinglist;
    public String buildinglistdate;
    public String citycode;
    public String plnid;
    public String adcontent;
    public String nplseqno;
    public String nplinnerseqno;
    public String uploadcount;
    public String adlength;

    public RealScheduleBean(){}

    public RealScheduleBean(String buildinglist, String buildinglistdate, String citycode, String plnid, String adcontent, String nplseqno, String nplinnerseqno, String uploadcount, String adlength) {
        this.buildinglist = buildinglist;
        this.buildinglistdate = buildinglistdate;
        this.citycode = citycode;
        this.plnid = plnid;
        this.adcontent = adcontent;
        this.nplseqno = nplseqno;
        this.nplinnerseqno = nplinnerseqno;
        this.uploadcount = uploadcount;
        this.adlength = adlength;
    }

    public String getBuildinglist() {
        return buildinglist;
    }

    public void setBuildinglist(String buildinglist) {
        this.buildinglist = buildinglist;
    }

    public String getBuildinglistdate() {
        return buildinglistdate;
    }

    public void setBuildinglistdate(String buildinglistdate) {
        this.buildinglistdate = buildinglistdate;
    }

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getPlnid() {
        return plnid;
    }

    public void setPlnid(String plnid) {
        this.plnid = plnid;
    }

    public String getAdcontent() {
        return adcontent;
    }

    public void setAdcontent(String adcontent) {
        this.adcontent = adcontent;
    }

    public String getNplseqno() {
        return nplseqno;
    }

    public void setNplseqno(String nplseqno) {
        this.nplseqno = nplseqno;
    }

    public String getNplinnerseqno() {
        return nplinnerseqno;
    }

    public void setNplinnerseqno(String nplinnerseqno) {
        this.nplinnerseqno = nplinnerseqno;
    }

    public String getUploadcount() {
        return uploadcount;
    }

    public void setUploadcount(String uploadcount) {
        this.uploadcount = uploadcount;
    }

    public String getAdlength() {
        return adlength;
    }

    public void setAdlength(String adlength) {
        this.adlength = adlength;
    }

    @Override
    public String toString() {
        return "RealScheduleBean{" +
                "buildinglist='" + buildinglist + '\'' +
                ", buildinglistdate='" + buildinglistdate + '\'' +
                ", citycode='" + citycode + '\'' +
                ", plnid='" + plnid + '\'' +
                ", adcontent='" + adcontent + '\'' +
                ", nplseqno='" + nplseqno + '\'' +
                ", nplinnerseqno='" + nplinnerseqno + '\'' +
                ", uploadcount='" + uploadcount + '\'' +
                ", adlength='" + adlength + '\'' +
                '}';
    }
}
