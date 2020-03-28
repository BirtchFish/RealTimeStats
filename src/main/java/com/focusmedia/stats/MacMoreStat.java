package com.focusmedia.stats;

import com.focusmedia.bean.MacScreenStat;

import com.focusmedia.util.JavaSparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MacMoreStat {

    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void main(String[] args){
        String appanme=args[0];
        String tdate=args[1];
        SparkSession session = JavaSparkUtil.getRemoteSparkSession(appanme, "parquet_table");
        try {
            generateMacMoreStat(session,tdate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    //基于mac_scan_log来统计每个城市中的楼宇数，设备数，广告数，广告主数，广告品牌数
    public static void generateMacMoreStat(SparkSession session,String tdate ) throws ParseException {

        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy_MM_dd");
        SimpleDateFormat mytime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parse = dateFormat.parse(tdate);
        String schedule_date=mytime.format(parse);

        //devid,mac,plninfo,scan_time,sch_version,pls_index,play_index,city_name,building_name,ad_content_id,adcontent,ad_length,adcustomer,adproduct
        Dataset<Row> mac_scan_stat = session.sql("select m.*,n.adcustomer,n.adproduct  from " +
                " (" +
                " select p.*,d.ad_content_id,d.ad_content as adcontent,d.ad_length from " +
                " (" +
                "   select t.*,case when d.city_name='北京索迪' then '北京' else d.city_name end as city_name,d.building_name from " +
                "   (select devid,mac,plninfo,scan_time from mac_scan_log where time='"+tdate+"') t" +
                "    left join " +
                "   (select devid,max(city_name) as city_name,max(building_name) as building_name from  fm_location where time='"+tdate+"' and install_status!='已拆除' group by devid) d" +
                "   on t.devid=d.devid where city_name is not null and city_name != '' " +
                " ) p  " +
                " left join " +
                " (select building_no,seq,pln_id,ad_content_id,max(ad_content) as ad_content , max(ad_length) as ad_length from schedule_screen_fam where time='"+tdate+"' and start_time<='"+schedule_date+"' and end_time>='"+schedule_date+"' group by building_no,seq,building_list,npl_seqno,npl_playindex,pln_id,ad_content_id) d " +
                " on p.plninfo=d.pln_id" +
                " ) m left join (select adcontent,adcustomer,adproduct,ad_content_id from distinct_videos where time='"+tdate+"' group by adcontent,adcustomer,adproduct,ad_content_id) n" +
                " on m.ad_content_id=n.ad_content_id");

        //mac_scan_stat.persist(StorageLevel.MEMORY_AND_DISK());

        mac_scan_stat.createOrReplaceTempView("tmp_macScan_stat");

        mac_scan_stat.printSchema();
        session.sql("select * from tmp_macScan_stat").show();

        //再城市前面加随机数，先部分group
        //session.sql("")


        //session.sql("select * from tmp_macScan_stat ").show(3);

        /*//城市和曝光人数
        Dataset<Row> mac_nums = session.sql("select city_name,count(1)  from (select distinct city_name,mac from tmp_macScan_stat) t group by city_name");
        List<Row> mac_nums_list = mac_nums.javaRDD().collect();
        System.out.println("城市和曝光人数"+mac_nums_list.size());

        //城市和楼宇数量
        Dataset<Row> building_nums = session.sql("select city_name,count(1) from (select distinct city_name,building_name from tmp_macScan_stat) t group by city_name");
        List<Row> building_nums_list = building_nums.javaRDD().collect();
        System.out.println("城市和楼宇数量"+building_nums_list.size());

        //城市和设备数量
        Dataset<Row> dev_nums = session.sql("select city_name,count(1) from (select distinct city_name,devid from tmp_macScan_stat) t group by city_name");
        List<Row> dev_nums_list = dev_nums.javaRDD().collect();
        System.out.println("城市和设备数量"+dev_nums_list.size());

        //城市和广告主数量
        Dataset<Row> customer_nums = session.sql("select city_name,count(1) from (select distinct city_name,adcustomer from tmp_macScan_stat) t group by city_name");
        List<Row> customer_nums_list = dev_nums.javaRDD().collect();
        System.out.println("城市和广告主数量"+customer_nums_list.size());

        //城市和广告品牌数量
        Dataset<Row> adproduct_nums = session.sql("select city_name,count(1) from (select distinct city_name,adproduct from tmp_macScan_stat) t group by city_name");
        List<Row> adproduct_nums_list = adproduct_nums.javaRDD().collect();
        System.out.println("城市和广告品牌数量"+adproduct_nums_list.size());

        //城市和广告创意数量
        Dataset<Row> adcontent_nums = session.sql("select city_name,count(1) from (select distinct city_name,adcontent from tmp_macScan_stat) t group by city_name");
        List<Row> adcontent_nums_list = adcontent_nums.javaRDD().collect();
        System.out.println("城市和广告创意数量"+adcontent_nums_list.size());*/


        //统计了 城市，曝光人数，楼宇数，设备数，广告主数，广告品牌数，广告创意数
        Dataset<Row> firststat = session.sql("select city_name,count(distinct(mac)) as personnums,count(distinct(building_name)) as buildnums, count(distinct(devid)) as devnums , " +
                " count(distinct(adcustomer)) as customernums,count(distinct(adproduct)) as brandnums ," +
                " count(distinct(adcontent)) as createnums " +
                " from" +
                " tmp_macScan_stat group by city_name");

        firststat.createOrReplaceTempView("mac_screen_stat");
        session.sql("select * from mac_screen_stat").show();

/*        //因为数据量不大，所以直接collect
        List<Row> firststat_list = firststat.collectAsList();


        List<MacScreenStat> result=new ArrayList<MacScreenStat>();
        for(Row line:firststat_list){
            System.out.println("Row "+line.toString());
            MacScreenStat screen=new MacScreenStat();
            screen.setCity_name(line.get(0).toString());
            screen.setExpose_person_nums(Integer.parseInt(line.get(1).toString()));
            screen.setBuild_nums(Integer.parseInt(line.get(2).toString()));
            screen.setDev_nums(Integer.parseInt(line.get(3).toString()));
            screen.setAdcustomer_nums(Integer.parseInt(line.get(4).toString()));
            screen.setBrand_nums(Integer.parseInt(line.get(5).toString()));
            screen.setAdcontent_nums(Integer.parseInt(line.get(6).toString()));

//            screen.setExpose_person_nums(Integer.parseInt(line.get(1).toString()));
//            //城市和楼宇数量
//            for(Row build:building_nums_list){
//                if(line.get(0).toString().equals(build.get(0).toString())){
//                    screen.setBuild_nums(Integer.parseInt(build.get(1).toString()));
//                }
//            }
//
//            //城市和设备数量
//            for(Row dev:dev_nums_list){
//                if(line.get(0).toString().equals(dev.get(0).toString())){
//                    screen.setDev_nums(Integer.parseInt(dev.get(1).toString()));
//                }
//            }
//
//            //城市和广告主数量
//            for(Row cus:customer_nums_list){
//                if(line.get(0).toString().equals(cus.get(0).toString())){
//                    screen.setAdcustomer_nums(Integer.parseInt(cus.get(1).toString()));
//                }
//            }
//
//            //城市和广告品牌数量
//            for(Row product:adproduct_nums_list){
//                if(line.get(0).toString().equals(product.get(0).toString())){
//                    screen.setBrand_nums(Integer.parseInt(product.get(1).toString()));
//                }
//            }
//
//            //城市和广告创意数量
//            for(Row adcontent:adcontent_nums_list){
//                if(line.get(0).toString().equals(adcontent.get(0).toString())){
//                    screen.setAdcontent_nums(Integer.parseInt(adcontent.get(1).toString()));
//                }
//            }

        screen.setDate(tdate);
        result.add(screen);
        System.out.println(screen.toString());
    }*/


        //统计到达两次或者两次以上的曝光人次数 没有去伪
        getTwoExposePerTimes(session, "tmp_macScan_stat","nofaketwo");

        //获取每个城市一次曝光人次数 没有去伪
        getOneExposePerTimes(session, "tmp_macScan_stat","nofakeone");

        //获取总的曝光人次数 没有去伪的
        Dataset<Row> city_expose_times = session.sql("select city_name,(twoexpose +oneexpose ) as nofake_expose_times from (select t.*,d.oneexpose from tmp_macScan_stat_nofaketwo t inner join tmp_macScan_stat_nofakeone d on t.city_name=d.city_name) p");
        city_expose_times.createOrReplaceTempView("tmp_cityexpose_times");
        session.sql("select * from tmp_cityexpose_times").show();

        ////////////////////////////////////////////////////////////////////////////////////////////
        //计算去伪之后的曝光人数和曝光人次
        Dataset<Row> fake_mac_scan = session.sql("select t.* from " +
                " (select *,substr(mac,0,6) as submac from tmp_macScan_stat) t " +
                " left join  db01.mac_factory b " +
                " on t.submac=b.mac_index_factory where b.factory_name is not null");

        fake_mac_scan.createOrReplaceTempView("tmp_fake_macscan");
        session.sql("select * from tmp_fake_macscan").show();


        //去伪 之后的曝光人数
        Dataset<Row> fake_city_nums = session.sql("select city_name,count(distinct(mac)) as fake_maccount from tmp_fake_macscan group by city_name");
        fake_city_nums.createOrReplaceTempView("fake_city_personnums");


        //去伪之后的两次或者两次以上的曝光人次
        getTwoExposePerTimes(session, "tmp_fake_macscan","faketwo");
        //去伪之后的一次曝光人次
        getOneExposePerTimes(session, "tmp_fake_macscan","fakeone");
        Dataset<Row> fake_expose_times = session.sql("select city_name,(oneexpose+twoexpose) as fake_exposetimes from (select t.*,d.oneexpose from tmp_fake_macscan_faketwo t inner join tmp_fake_macscan_fakeone d on t.city_name=d.city_name) p");
        fake_expose_times.createOrReplaceTempView("tmp_fake_exposetimes");


        //////////////////////////////////////////////////////////////////////////////////
        //计算这些广告的时长
        Dataset<Row> city_ad_length = session.sql("select city_name,sum(ad_length) as total_len from (select city_name,ad_content_id,max(ad_length) ad_length from tmp_macScan_stat group by city_name,ad_content_id) t group by city_name");
        city_ad_length.createOrReplaceTempView("tmp_city_adlength");


        Dataset<Row> result_statinfo = session.sql(" select '"+tdate+"' as day,m.*,n.nofake_expose_times,f_per_num.fake_maccount,s.fake_exposetimes,ad.total_len from mac_screen_stat m  " +
                " left join  tmp_cityexpose_times n on m.city_name=n.city_name " +
                " left join fake_city_personnums f_per_num on m.city_name=f_per_num.city_name " +
                " left join tmp_fake_exposetimes s on m.city_name=s.city_name" +
                " left join tmp_city_adlength ad on m.city_name=ad.city_name");


        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");



        result_statinfo.write().mode(SaveMode.Append).jdbc(URL,"mac_screen_stats",pro);


    }


    //获取两次或者两次曝光以上的人次数
    public static void getTwoExposePerTimes(SparkSession session,String src_table,String table_suffix){
        Dataset<Row> city_expose_twoabove = session.sql("select city_name,sum(arrivel*maccount) as twoexpose from" +
                " ( " +
                " select city_name,arrivel,sum(macnum) as maccount from " +
                " ( " +
                " select city_name,devid,arrivel,count(*) as macnum from " +
                " ( " +
                "  select city_name,devid, mac,count(*)+1 as arrivel from " +
                "  ( " +
                "  select city_name,devid, mac,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_time ,unix_timestamp(pre_scantime,'yyyy-MM-dd HH:mm:ss') as pre_scantime from " +
                "  ( " +
                "  select city_name,devid, mac,scan_time ,lag(scan_time,1) over(partition by city_name,devid, mac order by scan_time) as pre_scantime  from "+src_table+"  " +
                "   ) t  where t.scan_time-t.pre_scantime>300 " +
                "   ) group by city_name,devid, mac" +
                " ) m group by city_name,devid,arrivel" +
                " ) group by city_name,arrivel" +
                " ) group by city_name");

        city_expose_twoabove.createOrReplaceTempView(src_table+"_"+table_suffix);
        session.sql("select * from "+src_table+"_"+table_suffix).show();
        //List<Row> twoabove = city_expose_twoabove.collectAsList();
       // return twoabove;
    }

    //获取一次的曝光人次
    public static void getOneExposePerTimes(SparkSession session,String src_table,String suffix){
        Dataset<Row> city_onexpose = session.sql("select city_name,count(mac) as oneexpose from " +
                " ( " +
                "  select city_name,devid,mac,sum(flag) from " +
                "  (" +
                "   select city_name,devid,mac,scan_time,pre_scantime, case when pre_scantime is null then 0 when unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(pre_scantime,'yyyy-MM-dd HH:mm:ss')<=300 then 0 else 1 end as flag  from " +
                "  ( " +
                "   select city_name,devid,mac,scan_time,lag(scan_time,1) over (partition by city_name,devid,mac order by scan_time) as pre_scantime from "+src_table+" " +
                "   ) t " +
                "   ) s  group by city_name,devid,mac having sum(flag)=0 " +
                " ) h " +
                " group by city_name");


        city_onexpose.createOrReplaceTempView(src_table+"_"+suffix);
        session.sql("select * from "+src_table+"_"+suffix).show();
        //List<Row> oneexpose = city_onexpose.collectAsList();
       // return oneexpose;
    }

}
