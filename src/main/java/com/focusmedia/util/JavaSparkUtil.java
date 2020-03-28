package com.focusmedia.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class JavaSparkUtil {
    /*获取本地的SparkSession*/
    public static SparkSession getLocalSparkSession(String appname,String dbname){
        SparkConf conf=new SparkConf();
        conf.setAppName(appname).setMaster("local[*]");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        session.sql("use "+dbname);
        return session;
    }

    /*获取远程SparkSession*/
    public static SparkSession getRemoteSparkSession(String appname,String dbname){
        SparkConf conf=new SparkConf();
        conf.setAppName(appname);
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","800");
        conf.set("spark.sql.crossJoin.enabled","true");
        conf.set("spark.sql.shuffle.partitions", "800");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
       // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
        conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
        conf.set("spark.locality.wait","20s");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        session.sql("use "+dbname);

        //Broadcast<SparkSession> broadcast = context.broadcast(session);
        return session;
    }
}
