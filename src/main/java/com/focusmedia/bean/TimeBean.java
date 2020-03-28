package com.focusmedia.bean;

import java.io.Serializable;

public class TimeBean implements Serializable {

    public String date;
    public String time;

    public TimeBean(String date, String time) {
        this.date = date;
        this.time = time;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "TimeBean{" +
                "date='" + date + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
