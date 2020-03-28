package com.focusmedia.bean;

import java.io.Serializable;

public class MacRowBuilding implements Serializable {

    public String city_name;
    public String building_name;
    public String mac;
    public String scan_time;


    public MacRowBuilding(){}

    public MacRowBuilding(String city_name,String building_name, String mac, String scan_time) {
        this.city_name = city_name;
        this.building_name=building_name;
        this.mac = mac;
        this.scan_time = scan_time;
    }


    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getBuilding_name() {
        return building_name;
    }

    public void setBuilding_name(String building_name) {
        this.building_name = building_name;
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


