package com.focusmedia.bean;

import java.io.Serializable;
import java.util.List;

public class BuildDevTime implements Serializable {


    private String time;
    private List<String> devid_list;

    public BuildDevTime(){

    }

    public BuildDevTime(String time, List<String> devid_list) {
        this.time = time;
        this.devid_list = devid_list;
    }


    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public List<String> getDevid_list() {
        return devid_list;
    }

    public void setDevid_list(List<String> devid_list) {
        this.devid_list = devid_list;
    }
}
