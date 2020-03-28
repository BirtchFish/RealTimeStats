package com.focusmedia.stats;

import com.focusmedia.util.JavaSparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class RealStat {
    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void main(String[] args){

        String appname=args[0];

        SparkSession session = JavaSparkUtil.getRemoteSparkSession(appname, "parquet_table");
        try {
            generateFocusMediaStat(session);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        session.stop();
    }


    public static void generateFocusMediaStat(SparkSession session) throws ParseException {


        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        Date s=new Date();
        SimpleDateFormat time=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat format=new SimpleDateFormat("yyyy_MM_dd_HH");
        SimpleDateFormat zero=new SimpleDateFormat("yyyy_MM_dd");

        String tdate=format.format(s);  //2018_05_18_19
        String zero_date=zero.format(s)+"_00";  //2018_05_18_00
        String tdate2=time.format(s);   //2018-05-18 19:00:00
        //String fm_date=zero.format(s);  //2018_05_18

        //判断当前的long型是不是在00点和01点之间
        SimpleDateFormat long_date=new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat long_date2=new SimpleDateFormat("yyyy-MM-dd HH");
        long zero_long=long_date.parse(long_date.format(s)).getTime();
        long one_zero=long_date2.parse(long_date.format(s)+" 01").getTime();

        Dataset<Row> sql = session.sql("select max(time) from fm_location");
        String fm_date = sql.javaRDD().first().get(0).toString();

        if(s.getTime()>zero_long && s.getTime()<one_zero){
            //跑前一天的全量数量
            Calendar cal=Calendar.getInstance();
            cal.setTime(s);
            cal.add(Calendar.DATE,-1);
            tdate=zero.format(cal.getTime())+"_23";
            zero_date=zero.format(cal.getTime())+"_00";

            Dataset<Row> result = session.sql("select * from (select '" + tdate2 + "' as starttime,case when city_name ='北京索迪' then '北京' else city_name end as city_name, count(1) as playtimes,count(distinct(d.devid)) as devnums,sum(unix_timestamp(et,'yyyyMMddHHmmss')-unix_timestamp(st,'yyyyMMddHHmmss')) as allplay_length_real from " +
                    " ad_play_log  d left join ( select devid,max(city_name) as city_name from " +
                    "fm_location where time='" + fm_date + "' and install_status!='已拆除' group by devid) t on d.devid=t.devid where d.time >='"+zero_date+"'  and d.time <='"+tdate+"' group by t.city_name) t where city_name is not null");
            result.createOrReplaceTempView("tmp_result");

            result.write().mode(SaveMode.Overwrite).jdbc(URL,"time_focusmedia_stats",pro);
        }else{
            Dataset<Row> result = session.sql("select * from (select '" + tdate2 + "' as starttime,case when city_name ='北京索迪' then '北京' else city_name end as city_name, count(1) as playtimes,count(distinct(d.devid)) as devnums,sum(unix_timestamp(et,'yyyyMMddHHmmss')-unix_timestamp(st,'yyyyMMddHHmmss')) as allplay_length_real from " +
                    " ad_play_log  d left join ( select devid,max(city_name) as city_name from " +
                    "fm_location where time='" + fm_date + "' and install_status!='已拆除' group by devid) t on d.devid=t.devid where d.time >='"+zero_date+"'  and d.time <'"+tdate+"' group by t.city_name) t where city_name is not null");

            result.createOrReplaceTempView("tmp_result");

            result.write().mode(SaveMode.Overwrite).jdbc(URL,"time_focusmedia_stats",pro);
        }

    }
}
