package com.focusmedia.bean;

import scala.Serializable;

public class MacScreenStat implements Serializable {


    public String date;

    public String city_name;
    //曝光人数
    public int expose_person_nums;
    //曝光人次
    public int expose_person_times;

    //去伪之后的曝光人数
    public int fake_person_nums;
    //去伪之后的曝光人次
    public int fake_person_times;

    //广告主数
    public int adcustomer_nums;
    //品牌数
    public int  brand_nums;
    //广告创意数
    public int adcontent_nums;

    //楼宇数
    public int build_nums;

    //设备数
    public int dev_nums;

    //所有广告时长
    public int add_ad_length;

    public MacScreenStat(){}

    public MacScreenStat(String city_name, int expose_person_nums, int expose_person_times, int fake_person_nums, int fake_person_times, int adcustomer_nums, int brand_nums, int adcontent_nums, int build_nums, int dev_nums, int add_ad_length) {
        this.city_name = city_name;
        this.expose_person_nums = expose_person_nums;
        this.expose_person_times = expose_person_times;
        this.fake_person_nums = fake_person_nums;
        this.fake_person_times = fake_person_times;
        this.adcustomer_nums = adcustomer_nums;
        this.brand_nums = brand_nums;
        this.adcontent_nums = adcontent_nums;
        this.build_nums = build_nums;
        this.dev_nums = dev_nums;
        this.add_ad_length = add_ad_length;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public int getExpose_person_nums() {
        return expose_person_nums;
    }

    public void setExpose_person_nums(int expose_person_nums) {
        this.expose_person_nums = expose_person_nums;
    }

    public int getExpose_person_times() {
        return expose_person_times;
    }

    public void setExpose_person_times(int expose_person_times) {
        this.expose_person_times = expose_person_times;
    }

    public int getFake_person_nums() {
        return fake_person_nums;
    }

    public void setFake_person_nums(int fake_person_nums) {
        this.fake_person_nums = fake_person_nums;
    }

    public int getFake_person_times() {
        return fake_person_times;
    }

    public void setFake_person_times(int fake_person_times) {
        this.fake_person_times = fake_person_times;
    }

    public int getAdcustomer_nums() {
        return adcustomer_nums;
    }

    public void setAdcustomer_nums(int adcustomer_nums) {
        this.adcustomer_nums = adcustomer_nums;
    }

    public int getBrand_nums() {
        return brand_nums;
    }

    public void setBrand_nums(int brand_nums) {
        this.brand_nums = brand_nums;
    }

    public int getAdcontent_nums() {
        return adcontent_nums;
    }

    public void setAdcontent_nums(int adcontent_nums) {
        this.adcontent_nums = adcontent_nums;
    }

    public int getBuild_nums() {
        return build_nums;
    }

    public void setBuild_nums(int build_nums) {
        this.build_nums = build_nums;
    }

    public int getDev_nums() {
        return dev_nums;
    }

    public void setDev_nums(int dev_nums) {
        this.dev_nums = dev_nums;
    }

    public int getAdd_ad_length() {
        return add_ad_length;
    }

    public void setAdd_ad_length(int add_ad_length) {
        this.add_ad_length = add_ad_length;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "MacScreenStat{" +
                "date='" + date + '\'' +
                ", city_name='" + city_name + '\'' +
                ", expose_person_nums=" + expose_person_nums +
                ", expose_person_times=" + expose_person_times +
                ", fake_person_nums=" + fake_person_nums +
                ", fake_person_times=" + fake_person_times +
                ", adcustomer_nums=" + adcustomer_nums +
                ", brand_nums=" + brand_nums +
                ", adcontent_nums=" + adcontent_nums +
                ", build_nums=" + build_nums +
                ", dev_nums=" + dev_nums +
                ", add_ad_length=" + add_ad_length +
                '}';
    }
}
