package com.build.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/*
* 计算楼宇类型的所占比列
* */
public class BuildTypeFlow {

    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";

    //生成楼类型对应的平均人口数，平均人次数( 现在只算办公楼，公寓楼)
    public static void generateBuildFlow(SparkSession session,String tdate){

        //获取每个设备 对应mac的人次曝光次数 和 人数
        Dataset<Row> devid_time = session.sql(" " +
                " select devid,count(*) as exposetime ,count(distinct(mac)) as macnums  " +
                " from  tmp_twominute_mac_log_fake where time='" + tdate + "' " +
                " group by  devid  " +
                " ");
        devid_time.createOrReplaceTempView("tmp_devid_time");
        session.sql("select * from tmp_devid_time").show();

        //开始和fm_location和country_temp_table进行关联
        //先找出对应的devid,city_name,install_name
        Dataset<Row> dev_location = session.sql(" select t.devid,p.building_no,p.city_name,p.citycode,p.install_name,p.suit_kind from" +
                "   (select devid,max(update_time) as update_time from fm_location where time='" + tdate + "' group by devid) t" +
                "     left join" +
                "   (" +
                "   select t.*,q.citycode from" +
                "   (" +
                "   select devid,building_no,building_name,city_no,city_name,district_name,suit_kind,update_time,install_name" +
                "      from fm_location where time='" + tdate + "' and install_status='已安装' and device_style like '%互动%' and length(devid)=12" +
                "       group by devid,building_no,building_name,city_no,city_name,district_no,district_name,suit_kind,update_time,install_name" +
                "    ) t left join (select citycode,city from realtimeschedulescreenfam group by citycode,city) q on t.city_name=q.city" +
                "     ) p" +
                "    on t.devid=p.devid" +
                "   where t.update_time=p.update_time ");
        dev_location.createOrReplaceTempView("tmp_dev_location");
        session.sql("select * from tmp_dev_location").show();



        //再和country_temp_table进行关联找到对应的楼的类型
        Dataset<Row> buildingtype = session.sql("select  city,big_category,installed from csv_table.country_temp_table group by city,big_category,installed");
        buildingtype.createOrReplaceTempView("tmp_buildingtype");
        session.sql("select * from tmp_buildingtype").show();

        //获取每个设备所属的楼类型
        Dataset<Row> devid_buildtype = session.sql("select a.devid,a.city_name,b.big_category from tmp_dev_location a left join tmp_buildingtype b " +
                " on a.city_name=b.city and a.install_name = b.installed");
        devid_buildtype.createOrReplaceTempView("tmp_devid_buildtype");
        session.sql("select * from tmp_devid_buildtype").show();

        Dataset<Row> result = session.sql(" select city_name,big_category,sum(exposetime)/count(distinct(devid)) as avg_exposetime," +
                " sum(macnums)/count(distinct(devid)) as avg_macnums " +
                "  from (" +
                " select a.devid,a.exposetime,a.macnums,b.city_name,b.big_category " +
                " from  tmp_devid_time a left join tmp_devid_buildtype b " +
                " on a.devid=b.devid" +
                " ) group by city_name,big_category");
        result.createOrReplaceTempView("tmp_result");
        session.sql("select * from tmp_result").show();


        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        result.write().mode(SaveMode.Append).jdbc(URL,"city_buildtype_avg",pro);


    }

    public static void main(String[] args){

        String tdate=args[0];

        SparkConf conf=new SparkConf();
        conf.setAppName("generate_builttype_avg");
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","500");
        conf.set("spark.sql.crossJoin.enabled","true");
        conf.set("spark.sql.shuffle.partitions", "500");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
        // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
        conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
        conf.set("spark.locality.wait","20s");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        session.sql("use  parquet_table");
        generateBuildFlow(session,tdate);
        session.stop();
    }
}
