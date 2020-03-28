package com.focusmedia.bean;

import java.io.Serializable;

public class MacRow implements Serializable {

    public String devid;
    public String mac;
    public String scan_time;


    public MacRow(){}

    public MacRow(String devid, String mac, String scan_time) {
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
}


