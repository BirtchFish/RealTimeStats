package com.focusmedia.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DevDateUtil implements Serializable{
    public static Log log= LogFactory.getLog(DevDateUtil.class);

    public static ThreadLocal<SimpleDateFormat> timeformat=new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected  SimpleDateFormat initialValue(){
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public static SimpleDateFormat playformat=new SimpleDateFormat("yyyyMMddHHmmssSSS");

    public static ThreadLocal<SimpleDateFormat> shadeformat=new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    public static ThreadLocal<SimpleDateFormat> real_schedule=new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        }
    };

    public static ThreadLocal<SimpleDateFormat> dateformat=new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy_MM_dd");
        }
    };

    //将long型的时间转化为yyyy-MM-dd HH:mm:ss
    public static String playdatetoScan(long playlong){
        synchronized (DevDateUtil.class){
            Date playdate = new Date(playlong);
            return timeformat.get().format(playdate);
        }
    }

    //将yyyy_MM_dd字符串转化为long型数据
    public static long dateStrToLong(String date){
        long mytime=0L;
        try {
            mytime = dateformat.get().parse(date).getTime();
        } catch (ParseException e) {
            log.error("yyyy_MM_dd 日期格式转化错误");
        }
        return mytime;
    }
    //将字符串yyyy-MM-dd HH:mm:ss转化为long型数据
    public static long timeStrToLong(String mytime){
        long timelong=0L;
        try {
            synchronized (DevDateUtil.class){
                timelong = timeformat.get().parse(mytime).getTime();
            }
        } catch (ParseException e) {
            log.error("yyyy-MM-dd HH:mm:ss 日期格式转化错误");
        }
        return timelong;
    }

    //将long型数字转化为yyyy-MM-dd HH:mm:ss字符串格式
    public static  String longTimeToStr(long timestr){
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(timestr);
        String format="";
        synchronized (DevDateUtil.class){
            format = timeformat.get().format(instance.getTime());
        }
        return format;
    }

    //将long型数字转化为yyyy_MM_dd字符串格式
    public static String longDateToStr(long datestr){
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(datestr);
        return dateformat.get().format(instance.getTime());
    }

    public static String shadelongDateToStr(long datestr){
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(datestr);
        return shadeformat.get().format(instance.getTime());
    }

    //将long型数字转化为yyyy/MM/dd HH:mm:ss字符串格式
    public static  String reallongTimeToStr(long timestr){
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(timestr);
        return real_schedule.get().format(instance.getTime());
    }


    //返回当前日期的早晨6点
    public static long getSixHourLong(String tdate){
        long result=0;
        try {
            Date parse = dateformat.get().parse(tdate);
            Calendar cal=Calendar.getInstance();
            cal.setTime(parse);
            cal.set(Calendar.HOUR,6);
            //System.out.println(cal.getTime());
            result= cal.getTime().getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

}
