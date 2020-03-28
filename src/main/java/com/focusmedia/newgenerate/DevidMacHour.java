package com.focusmedia.newgenerate;

import java.io.Serializable;

public class DevidMacHour implements Serializable {

    public String devid;
    public String mac;
    public String hour;

    public DevidMacHour() {
    }

    public DevidMacHour(String devid, String mac, String hour) {
        this.devid = devid;
        this.mac = mac;
        this.hour = hour;
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

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }
}
