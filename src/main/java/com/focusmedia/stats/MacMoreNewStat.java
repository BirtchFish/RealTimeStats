package com.focusmedia.stats;

import com.focusmedia.bean.MacRow;
import com.focusmedia.util.JavaSparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

public class MacMoreNewStat {

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
        session.stop();
    }

    //基于mac_scan_log来统计每个城市中的楼宇数，设备数，广告数，广告主数，广告品牌数
    public static void generateMacMoreStat(SparkSession session,String tdate ) throws ParseException {

        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy_MM_dd");
        SimpleDateFormat mytime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parse = dateFormat.parse(tdate);
        String schedule_date=mytime.format(parse);

        //devid,plninfo,sch_version,pls_index,play_index,city_name,building_name
        Dataset<Row> mac_scan_stat = session.sql("select t.*,case when d.city_name='北京索迪' then '北京' else d.city_name end as city_name,d.building_name from " +
                "   (" +
                "   select devid,pid as plninfo ,case when length(sv)>=7 then substr(sv,0,6) else sv end as sch_version,pi as pls_index,idx as play_index from ad_play_log where time like '"+tdate+"%' and sv!='0'  group by devid,inner_sn,master,pid,st,et,sv,pi,idx" +
                "    ) t" +
                "   left join " +
                "   (" +
                "    select devid,max(city_name) as city_name,max(building_name) as building_name from  " +
                "      fm_location where time='"+tdate+"' and install_status!='已拆除' and substr(device_style,1,6)='[智能互动]' group by devid" +
                "   ) d  on t.devid=d.devid where city_name is not null and city_name != '' ");

        //mac_scan_stat.persist(StorageLevel.MEMORY_AND_DISK());
        //devid,plninfo,sch_version,pls_index,play_index,city_name,building_name
        mac_scan_stat.createOrReplaceTempView("tmp_macScan_stat");
        //session.sql("select * from tmp_macScan_stat").show();



        //计算city_设备数
        Dataset<Row> city_devnums = session.sql("select city_name,count(1) as devcount from " +
                " (" +
                " select city_devid,split(city_devid,'#')[0] as city_name,split(city_devid,'#')[1] as devid from " +
                " (" +
                "  select *,concat(concat(city_name,'#'),devid) as city_devid from  tmp_macScan_stat" +
                " ) group by city_devid" +
                " ) group by city_name");

        city_devnums.createOrReplaceTempView("tmp_city_devnums");
        //session.sql("select * from tmp_city_devnums").show();

        //计算city_楼宇数
        Dataset<Row> city_buildnums = session.sql("select city_name,count(1) as buildcount from " +
                " (" +
                " select city_buildname,split(city_buildname,'#')[0] as city_name,split(city_buildname,'#')[1] as devid from " +
                " (" +
                "  select *,concat(concat(city_name,'#'),building_name) as city_buildname from  tmp_macScan_stat" +
                " ) group by city_buildname" +
                " ) group by city_name");
        city_buildnums.createOrReplaceTempView("tmp_city_buildnums");
       // session.sql("select * from tmp_city_buildnums").show();


        //再和schedule_screen_fam进行关联后拿到ad_content_id，再和distinct_videos进行关联
        //devid,plninfo,sch_version,pls_index,play_index,city_name,building_name,joinkey,ad_content_id,ad_length
        Dataset<Row> middle_mac_scan = session.sql("select m.*,p.ad_content_id,p.ad_length from " +
                " (select *, concat(concat(concat(sch_version,pls_index),play_index),plninfo) as joinkey from tmp_macScan_stat) m " +
                " left join " +
                " (" +
                " select concat(concat(concat(building_list,npl_seqno),npl_playindex),pln_id) as joinkey,ad_content_id,ad_length from " +
                " (" +
                " select  building_list,npl_seqno,npl_playindex,pln_id,max(ad_content_id) as ad_content_id,max(ad_length) as ad_length from schedule_screen_fam where time='"+tdate+"' and daybefore='0' and screen='2' " +
                "    and start_time<='"+schedule_date+"' and end_time>='"+schedule_date+"' " +
                "  group by  city,building_list,npl_seqno,npl_playindex,pln_id " +
                " ) t" +
                " ) p on m.joinkey=p.joinkey where p.ad_content_id is not null");
        middle_mac_scan.createOrReplaceTempView("tmp_middle_macscan");
        //session.sql("select * from tmp_middle_macscan").show();


        //计算城市对应的各个广告的时长
        Dataset<Row> city_adlength = session.sql("select city_name,sum(ad_length) as ad_totallen  from (select city_name,cast(ad_length as double) from tmp_middle_macscan ) group by city_name ");
        city_adlength.createOrReplaceTempView("tmp_city_adlength");
       // session.sql("select * from tmp_city_adlength").show();




        //再和distinct_videos进行关联并计算广告主数，广告品牌数，广告创意数
        //devid,plninfo,sch_version,pls_index,play_index,city_name,building_name,joinkey,ad_content_id,ad_length,adcontent,adcustomer,adproduct
        Dataset<Row> rdd_distinct_videos = session.sql("select s.*,d.adcontent,d.adcustomer,d.adproduct from tmp_middle_macscan s inner join " +
                " (select adcontent,adcustomer,adproduct,ad_content_id from distinct_videos where time='" + tdate + "' " +
                "  group by adcontent,adcustomer,adproduct,ad_content_id ) d on s.ad_content_id = d.ad_content_id ");
        rdd_distinct_videos.createOrReplaceTempView("tmp_adcustomer_mac");
        //session.sql("select * from tmp_adcustomer_mac").show();

        //计算广告主数
        Dataset<Row> city_adcustomer_nums = session.sql(" select city_name,count(1) as city_adcustomer_nums from (" +
                " select city_cus,split(city_cus,'#')[0] as city_name,split(city_cus,'#')[1] as adcustomer from " +
                " (select concat(concat(city_name,'#'),adcustomer) as city_cus from tmp_adcustomer_mac) t group by t.city_cus ) group by city_name");

        city_adcustomer_nums.createOrReplaceTempView("tmp_city_adcustomer_nums");
        //session.sql("select * from tmp_city_adcustomer_nums").show();

        //计算广告品牌数
        Dataset<Row> city_brand_nums = session.sql(" select city_name,count(1) as city_adproduct_nums from (" +
                " select city_adpro,split(city_adpro,'#')[0] as city_name,split(city_adpro,'#')[1] as adproduct from " +
                " (select concat(concat(city_name,'#'),adproduct) as city_adpro from tmp_adcustomer_mac) t group by t.city_adpro ) group by city_name");

        city_brand_nums.createOrReplaceTempView("tmp_city_brand_nums");
        //session.sql("select * from tmp_city_brand_nums").show();

        //计算广告创意数
        Dataset<Row> city_content_nums = session.sql(" select city_name,count(1) as city_adcontent_nums from (" +
                " select city_content,split(city_content,'#')[0] as city_name,split(city_content,'#')[1] as adcontent from " +
                " (select concat(concat(city_name,'#'),adcontent) as city_content from tmp_adcustomer_mac) t group by t.city_content ) group by city_name");

        city_content_nums.createOrReplaceTempView("tmp_city_content_nums");
       // session.sql("select * from tmp_city_content_nums").show();



        //mac曝光人数和曝光次数,根据mac_scan_log计算曝光人数
        //devid,mac,scan_time
        generateOneOrTwoExposeMac(session,tdate,150);

        //与楼宇进行关联找到城市
        //获取了当天一次到达和大于等于两次到达的mac的对应的devid,mac,scan_time 记录
        Dataset<Row> oneAndTwo_expose = session.sql("select devid,mac,scan_time,case when city_name='北京索迪' then '北京' else city_name end as city_name from " +
                " (" +
                " select t.*,p.city_name from tmp_expose t " +
                " left join " +
                " (" +
                " select devid,max(city_name) as city_name,max(building_name) as building_name from  " +
                " fm_location where time='" + tdate + "' and install_status!='已拆除' and substr(device_style,1,6)='[智能互动]' group by devid" +
                " ) p  on t.devid=p.devid where city_name is not null " +
                " ) s ");

        oneAndTwo_expose.createOrReplaceTempView("tmp_on_two_macinfo");

        //获取未去伪的曝光人数
        Dataset<Row> nofake_maccount = session.sql("select city_name,count(distinct(mac)) as nofake_maccount from tmp_on_two_macinfo group by city_name");
        nofake_maccount.createOrReplaceTempView("tmp_nofake_maccount");

        //获取未去伪的曝光人次数
        Dataset<Row> nofake_mactime = session.sql("select city_name,count(mac) as nofake_mactime from tmp_on_two_macinfo group by city_name");
        nofake_mactime.createOrReplaceTempView("tmp_nofake_mactime");


        //进行去伪后计算曝光人数和曝光人次数
        Dataset<Row> fake_datainfo = session.sql("select t.devid,t.mac,t.scan_time,t.city_name from " +
                " (select *,substr(mac,0,6) as submac from  tmp_on_two_macinfo ) t" +
                " left join " +
                " db01.mac_factory as p on t.submac=p.mac_index_factory where factory_name is not null");
        fake_datainfo.createOrReplaceTempView("tmp_fake_datainfo");

        //获取去伪后的曝光人数
        Dataset<Row> fake_maccount = session.sql("select city_name,count(distinct(mac)) as fake_maccount from tmp_fake_datainfo group by city_name");
        fake_maccount.createOrReplaceTempView("tmp_fake_maccount");

        //获取去伪后的曝光人次数
        Dataset<Row> fake_mactime = session.sql("select city_name,count(mac) as fake_mactime from tmp_fake_datainfo group by city_name");
        fake_mactime.createOrReplaceTempView("tmp_fake_mactime");



        Dataset<Row> result_statinfo = session.sql(" select '"+tdate+"' as day,a.city_name,a.devcount,b.buildcount,c.ad_totallen ,d.city_adcustomer_nums," +
                " e.city_adproduct_nums,f.city_adcontent_nums,g.nofake_maccount," +
                " h.nofake_mactime,j.fake_maccount,k.fake_mactime" +
                " from tmp_city_devnums a " +
                " left join tmp_city_buildnums b on a.city_name=b.city_name " +
                " left join tmp_city_adlength c on a.city_name=c.city_name " +
                " left join tmp_city_adcustomer_nums d on a.city_name=d.city_name " +
                " left join tmp_city_brand_nums e on a.city_name=e.city_name " +
                " left join tmp_city_content_nums f on a.city_name=f.city_name " +
                " left join tmp_nofake_maccount g on a.city_name=g.city_name" +
                " left join tmp_nofake_mactime h on a.city_name=h.city_name" +
                " left join tmp_fake_maccount j on a.city_name=j.city_name " +
                " left join tmp_fake_mactime k on a.city_name=k.city_name");
        result_statinfo.createOrReplaceTempView("tmp_result");

        //session.sql("select * from tmp_result").show();
        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        result_statinfo.write().mode(SaveMode.Append).jdbc(URL,"mac_screen_stats_tversion",pro);

    }




    public static void generateOneOrTwoExposeMac(SparkSession session,String tdate,int seconds){
        //获取到达两次或者两次以上的devid,mac,time记录
        Dataset<Row> base_data = session.sql("select devid,mac,scan_time,pre_scan_time, scan_new,pre_new from " +
                " (" +
                "  select devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "   (" +
                "     select devid,mac,scan_time,lag(scan_time,1) over ( partition by devid,mac order by scan_time) as pre_scan_time" +
                "       from mac_scan_log where time='" + tdate + "'" +
                "    )  t " +
                " ) p where scan_new-pre_new>" + seconds + " ");

        JavaPairRDD<String, Row> devid_row = base_data.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                // devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                return new Tuple2<String, Row>(row.get(0).toString(), row);
            }
        });

        //生成到达次数的mac记录
        JavaRDD<MacRow> macRowJavaRDD = devid_row.groupByKey().flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, MacRow>() {
            @Override
            public Iterator<MacRow> call(Tuple2<String, Iterable<Row>> devid_row) throws Exception {

                List<MacRow> result = new ArrayList<MacRow>();

                final Map<String, List<Row>> mac_lines = new HashMap<String, List<Row>>();

                String devid = devid_row._1;

                //因为怕这里数据量大，就先按照每个mac来进行划分来进行比较
                devid_row._2.forEach(new Consumer<Row>() {
                    @Override
                    public void accept(Row row) {
                        // devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                        if (mac_lines.get(row.get(1).toString()) == null || mac_lines.get(row.get(1).toString()).size() == 0) {
                            List<Row> mac_list = new ArrayList<Row>();
                            mac_list.add(row);
                            mac_lines.put(row.get(1).toString(), mac_list);
                        } else {
                            mac_lines.get(row.get(1).toString()).add(row);
                        }
                    }
                });

                //正对每个mac下的到达次数的记录进行处理
                for (Map.Entry<String, List<Row>> mac_entry : mac_lines.entrySet()) {
                    List<MacRow> mac_rows = new ArrayList<MacRow>();
                    String mac = mac_entry.getKey();


                    List<Row> rows = mac_entry.getValue();
                    rows.sort(new Comparator<Row>() {
                        @Override
                        public int compare(Row o1, Row o2) {
                            long one=Long.parseLong(o1.get(4).toString());
                            long two=Long.parseLong(o2.get(4).toString());
                            return Long.compare(one,two);
                        }
                    });

                    int tmp_index=0;
                    Row pre=null;
                    for (Row line : rows) {
                        // devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                        if(tmp_index==0){
                            //代表第一条
                            MacRow scan_row=new MacRow(devid,line.get(1).toString(),line.get(2).toString());
                            MacRow pre_row=new MacRow(devid,line.get(1).toString(),line.get(3).toString());
                            mac_rows.add(scan_row);
                            mac_rows.add(pre_row);

                        }else{

                            if(pre!=null){
                                //获取上一条的scan_new,和当前条的pre_new进行比较
                                long pre_scan_new=Long.parseLong(pre.get(4).toString());
                                long cur_pre_new=Long.parseLong(line.get(5).toString());
                                if(cur_pre_new-pre_scan_new>seconds){
                                    //代表没有挨着直接将两条添加进去
                                    MacRow scan_row=new MacRow(devid,line.get(1).toString(),line.get(2).toString());
                                    MacRow pre_row=new MacRow(devid,line.get(1).toString(),line.get(3).toString());
                                    mac_rows.add(scan_row);
                                    mac_rows.add(pre_row);

                                }else{
                                    MacRow scan_row=new MacRow(devid,line.get(1).toString(),line.get(2).toString());
                                    mac_rows.add(scan_row);
                                }
                            }

                        }
                        tmp_index++;
                        pre=line;
                    }
                    result.addAll(mac_rows);
                }

                return result.iterator();
            }
        });

        Dataset<Row> dataFrame = session.createDataFrame(macRowJavaRDD, MacRow.class);

        //计算一次到达的那些mac记录

        Dataset<Row> one_reach = session.sql("select devid,mac, max(scan_time) as scan_time from " +
                " (" +
                " select devid,mac,scan_time,case when scan_new-pre_new<" + seconds + " then 0 else 1 end as flag  from " +
                " (" +
                " select devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "  (" +
                "    select devid,mac,scan_time,lag(scan_time,1) over ( partition by devid,mac order by scan_time) as pre_scan_time" +
                "     from mac_scan_log where time='" + tdate + "'" +
                "  ) t " +
                " )" +
                " ) group by devid,mac having sum(flag)=0");

        Dataset<Row> all_devidmac = dataFrame.union(one_reach);

        all_devidmac.createOrReplaceTempView("tmp_expose");
    }




    //获取两次或者两次曝光以上的人次数
    public static void getTwoExposePerTimes(SparkSession session,String src_table,String table_suffix){
        Dataset<Row> city_expose_twoabove = session.sql("select city_name,sum(maccount) as twoexpose from " +
                " (" +
                " select city_name,arrivel,sum(macnum) as maccount from " +
                " (" +
                " select city_name,devid,arrivel,count(*) as macnum from  " +
                " (" +
                " select city_name,devid,mac,count(*)+1 as arrivel from  " +
                " (" +
                " select city_name,devid, mac,scan_time_new-pre_scantime_new from " +
                " ( " +
                "  select city_name,devid, mac,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_time_new , case when pre_scantime is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else  unix_timestamp(pre_scantime,'yyyy-MM-dd HH:mm:ss') end as pre_scantime_new from " +
                "     ( " +
                "        select city_name,devid, mac,scan_time ,lag(scan_time,1) over(partition by city_name,devid, mac order by scan_time) as pre_scantime  from tmp_macScan_stat  " +
                "      ) t   " +
                " ) where scan_time_new-pre_scantime_new>=300 " +
                " ) group by city_name,devid,mac " +
                " ) group by city_name,devid,arrivel" +
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
                "   select city_name,devid,mac,scan_time,pre_scantime, case when pre_scantime is null then 0 when unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(pre_scantime,'yyyy-MM-dd HH:mm:ss')<300 then 0 else 1 end as flag  from " +
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
