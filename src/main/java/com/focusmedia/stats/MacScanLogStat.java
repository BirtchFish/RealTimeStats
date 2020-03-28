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



public class MacScanLogStat {

    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void main(String[] args){

        String appname=args[0];

        String tdate=args[1];

        SparkSession session = JavaSparkUtil.getRemoteSparkSession(appname, "parquet_table");

        try {
            if(args.length>2){
                generateFocusMediaStat(session,tdate,args[2]);
            }else{
                generateFocusMediaStat(session,tdate,null);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        session.stop();
    }

    //统计前三天的mac_scan_log中的城市，mac总数，和去重的mac总数
    public static void generateFocusMediaStat(SparkSession session,String tdate,String fm_date) throws ParseException {


        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        String fmdate=fm_date;
        if(fm_date==null || fm_date.equals("")){
            Dataset<Row> sql = session.sql("select max(time) from fm_location");
            fmdate= sql.javaRDD().first().get(0).toString();
        }

        Dataset<Row> result = session.sql("select '"+tdate+"' as day, t.city_name,count(1) as maccount ,count(distinct(mac)) as single_maccount " +
                "from (select mac ,devid from mac_scan_log where time='"+tdate+"'  ) d left join" +
                " (select devid,max(city_name) as city_name from  fm_location where time='"+fmdate+"' and install_status!='已拆除' group by devid) t " +
                "on d.devid=t.devid group by t.city_name");
        result.write().mode(SaveMode.Append).jdbc(URL,"macscan_focusmedia_stats",pro);

    }
}
