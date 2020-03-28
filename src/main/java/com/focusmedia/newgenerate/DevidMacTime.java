package com.focusmedia.newgenerate;

import java.io.Serializable;

public class DevidMacTime implements Serializable {


    public String devid;
    public String mac;
    public String time_diff;
    public String start_time;
    public long start_time_long_new;

    public DevidMacTime() {
    }


    public DevidMacTime(String devid, String mac, String time_diff,
                        String start_time, long start_time_long_new) {
        this.devid = devid;
        this.mac = mac;
        this.time_diff = time_diff;
        this.start_time = start_time;
        this.start_time_long_new = start_time_long_new;
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

    public String getTime_diff() {
        return time_diff;
    }

    public void setTime_diff(String time_diff) {
        this.time_diff = time_diff;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public long getStart_time_long_new() {
        return start_time_long_new;
    }

    public void setStart_time_long_new(long start_time_long_new) {
        this.start_time_long_new = start_time_long_new;
    }
}
