package com.focusmedia.stats;

import com.focusmedia.util.JavaSparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/*
* 基于ad_play_info来统计每个城市的实际播放次数
* */
public class FactVersionTime {

    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void generateFactPlaytimes(SparkSession session,String tdate){

        String linedate=tdate.replace("_","-");

        Dataset<Row> ad_play = session.sql("select citycode,devid,inner_sn,city_name,sch_version,max(uploadcount) as uploadcount  from ad_play_info " +
                " where time='"+tdate+"'  and city_name is not null " +
                " group  by citycode,devid,inner_sn,city_name,sch_version");

        ad_play.createOrReplaceTempView("tmp_ad_play");

        //获取对应的城市，版本，批次下的basefrequency,
        Dataset<Row> realtimebuildinglist = session.sql(
                " select buildinglist,citycode,packagecode,uploadcount,max(sumadlength) as adlength from realtimebuildinglist" +
                " where sumadlength is not null" +
                " group by buildinglist,citycode,packagecode,uploadcount" );
        realtimebuildinglist.createOrReplaceTempView("tmp_realtimebuildinglist");


        Dataset<Row> result_realbuildinglist = session.sql(" select t.citycode,t.buildinglist,t.uploadcount,k.basefrequency" +
                " from tmp_realtimebuildinglist t left join " +
                "     (select  buildinglist,citycode,packagecode,uploadcount,sumadlength,basefrequency from " +
                "          realtimebuildinglist where sumadlength is not null group by buildinglist,citycode,packagecode,uploadcount,sumadlength,basefrequency) k" +
                "  on  t.citycode=k.citycode and t.buildinglist=k.buildinglist and t.uploadcount=k.uploadcount" +
                "  and t.packagecode=k.packagecode and t.adlength=k.sumadlength");
        result_realbuildinglist.createOrReplaceTempView("tmp_result_realbuildinglist");


        //获取对应的排期数据
        Dataset<Row> schedule = session.sql("select citycode,buildinglist,uploadcount, count(*) as adcount from" +
                "  (" +
                " select buildinglist,citycode,nplseqno,nplinnerseqno,uploadcount" +
                "  from realtimeschedulescreenfam_by_day where buildinglistdate='" + linedate + "' " +
                "    group by buildinglist,citycode,nplseqno,nplinnerseqno,uploadcount" +
                "   ) t group by citycode,buildinglist,uploadcount");
        schedule.createOrReplaceTempView("tmp_schedule");


        Dataset<Row> middle_result = session.sql(" select city_name,sum(ft) as playtimes " +
                " from ( " +
                "    select citycode,city_name,devid,inner_sn,(adcount*basefrequency) as ft " +
                "    from " +
                "    (" +
                "       select h.citycode,h.city_name,h.devid,h.inner_sn,h.adcount,p.basefrequency " +
                "       from " +
                "       ( " +
                "         select a.citycode,a.city_name,a.devid,a.inner_sn,a.sch_version,a.uploadcount," +
                "         b.adcount" +
                "         from  tmp_ad_play a left join tmp_schedule b " +
                "            on a.citycode=b.citycode and a.sch_version=b.buildinglist and a.uploadcount=b.uploadcount" +
                "       ) h left join tmp_result_realbuildinglist p" +
                "     on h.citycode=p.citycode and h.sch_version=p.buildinglist and h.uploadcount=p.uploadcount" +
                "    ) r" +
                " ) g  group by city_name"
               );

        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        middle_result.write().mode(SaveMode.Append).jdbc(URL,"fact_version_times_four",pro);
    }

    public static void main(String[] args){
        String tdate=args[0];
        SparkSession session = JavaSparkUtil.getRemoteSparkSession("fact_version_times", "parquet_table");
        generateFactPlaytimes(session,tdate);
        session.stop();
    }

}
