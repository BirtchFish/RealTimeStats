package com.full.flow;

import com.focusmedia.util.DevDateUtil;
import com.focusmedia.util.JavaSparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class FullFlowStatDay {
    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void getFullFlowResult(SparkSession session,String tdate,String linedate){

        //获取对应的城市，版本，批次下的最大的sumadlength
        Dataset<Row> realtimebuildinglist = session.sql("select buildinglist,citycode,uploadcount,max(adlength) ad_length from " +
                " (" +
                " select buildinglist,citycode,packagecode,uploadcount,max(sumadlength) as adlength from realtimebuildinglist" +
                " where sumadlength is not null" +
                " group by buildinglist,citycode,packagecode,uploadcount" +
                " ) t group by buildinglist,citycode,uploadcount");
        realtimebuildinglist.createOrReplaceTempView("tmp_realtimebuildinglist");


        Dataset<Row> result_realbuildinglist = session.sql(" select t.citycode,t.buildinglist,t.uploadcount,k.basefrequency" +
                " from tmp_realtimebuildinglist t left join " +
                "     (select  buildinglist,citycode,uploadcount,sumadlength,basefrequency from " +
                "          realtimebuildinglist group by buildinglist,citycode,uploadcount,sumadlength,basefrequency) k" +
                "  on t.buildinglist=k.buildinglist and t.citycode=k.citycode and t.uploadcount=k.uploadcount" +
                " and t.ad_length=k.sumadlength");
        result_realbuildinglist.createOrReplaceTempView("tmp_result_realbuildinglist");


        //读取 realtimeschedulescreenfam_by_day 实时排期表 获取每个批次中广告每天的播放次数
        Dataset<Row> ad_playtimes = session.sql("select citycode,buildinglistdate,buildinglist,uploadcount,adcontent,count(1) as playtimes from" +
                " (" +
                " select buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno,nplinnerseqno,uploadcount,adlength" +
                " from realtimeschedulescreenfam_by_day where  buildinglistdate = '"+linedate+"'" +
                " group by buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno,nplinnerseqno,uploadcount,adlength" +
                " ) t group by citycode,buildinglistdate,buildinglist,uploadcount,adcontent");
        ad_playtimes.createOrReplaceTempView("tmp_ad_playtimes");


        Dataset<Row> middle_ad_playtimes = session.sql("select g.*,n.basefrequency from tmp_ad_playtimes g left join tmp_result_realbuildinglist n" +
                " on g.buildinglist=n.buildinglist and g.citycode=n.citycode and g.uploadcount=n.uploadcount");
        middle_ad_playtimes.createOrReplaceTempView("tmp_middle_adplay_times");

        Dataset<Row> result_ad_playtimes = session.sql("select citycode,buildinglistdate,buildinglist,uploadcount,adcontent ," +
                "  case when basefrequency is null then playtimes*60 else playtimes*basefrequency end as play_nums " +
                "    from tmp_middle_adplay_times");

        result_ad_playtimes.createOrReplaceTempView("tmp_result_adplay_times");




        //获取当前城市 中 版本的最大批次数
        // 以便替换那些版本不为0，但是批次号为0的 批次号
        Dataset<Row> buildlist_uploadcount = session.sql("select citycode,buildinglist,max(uploadcount) from " +
                " (" +
                " select buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno,nplinnerseqno,uploadcount,adlength" +
                "  from realtimeschedulescreenfam_by_day where buildinglistdate='" + linedate + "'" +
                " group by buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno,nplinnerseqno,uploadcount,adlength" +
                " ) group by citycode,buildinglist");
        List<Row> version_uploadcount = buildlist_uploadcount.collectAsList();
        Map<String,String> city_version_uploadcount=new HashMap<String,String>();
        for(Row line:version_uploadcount){
            //生成城市编号，版本  和  批次的对应关系
            city_version_uploadcount.put(line.get(0).toString()+","+line.get(1).toString(),line.get(2).toString());
        }


        //读取 ad_info 获取ad_content_id 和ad_content 的对应关系 数据量不大直接collect
        Dataset<Row> sql = session.sql("select adcontent,adcontent_id from ad_info group by adcontent,adcontent_id");
        List<Row> content_conid = sql.collectAsList();
        final Map<String,String> map_id_content=new HashMap<String,String>();
        for(Row line:content_conid){
            //用来保存ad_content_id到ad_content的对应关系
            map_id_content.put(line.get(1).toString(),line.get(0).toString());
        }


        //当mac_view_info中的ad_content_id为空时，则需要按照plninfo来进行查找,数据量不大直接collect
        Dataset<Row> pln_adcontent = session.sql(" select  citycode,plnid,adcontent" +
                "  from realtimeschedulescreenfam_by_day where buildinglistdate='" + linedate + "'" +
                "  group by citycode,plnid,adcontent");
        List<Row> rows = pln_adcontent.collectAsList();
        final Map<String,String> city_pln_adcontent=new HashMap<String,String>();
        for(Row line:rows){
            //用来保存citycode+plnid  到 adcontent  的对应关系
            city_pln_adcontent.put(line.get(0).toString()+","+line.get(1).toString(),line.get(2).toString());
        }


        //获取mac_view_info数据,并补齐对应的adcontent
        Dataset<Row> mac_view_data = session.sql("select citycode,city_name,mac,scan_time,sch_version,uploadcount,pls_index,play_index,plninfo,ad_content_id,time " +
                " from mac_view_info where time='" + tdate + "' and sch_version is not null ");
        JavaRDD<MacViewExtend> macextend = mac_view_data.toJavaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>, MacViewExtend>() {
            @Override
            public Iterator<MacViewExtend> call(Iterator<Row> rowIterator) throws Exception {
                //citycode,city_name,mac,scan_time,sch_version,uploadcount,pls_index,play_index,plninfo,ad_content_id
                List<MacViewExtend> result_list = new ArrayList<MacViewExtend>();
                while (rowIterator.hasNext()) {
                    Row next = rowIterator.next();

                    String citycode = next.isNullAt(0) ? null : next.get(0).toString();
                    String city_name = next.isNullAt(1) ? null : next.get(1).toString();
                    String mac = next.isNullAt(2) ? null : next.get(2).toString();
                    String scan_time = next.isNullAt(3) ? null : next.get(3).toString();
                    String sch_version = next.isNullAt(4) ? null : next.get(4).toString();
                    String uploadcount = next.isNullAt(5) ? null : next.get(5).toString();
                    String pls_index = next.isNullAt(6) ? null : next.get(6).toString();
                    String play_index = next.isNullAt(7) ? null : next.get(7).toString();
                    String plninfo = next.isNullAt(8) ? null : next.get(8).toString();
                    String ad_content_id = next.isNullAt(9) ? null : next.get(9).toString();
                    String time = next.isNullAt(10) ? null : next.get(10).toString();
                    MacViewExtend extend = new MacViewExtend(citycode, city_name, mac, scan_time,
                            sch_version, uploadcount, pls_index, play_index, plninfo, ad_content_id,time, null, null
                    );
                    if (next.isNullAt(9) || next.get(9).toString().equals("NULL")) {
                        //如果adcontentid为空，则根据citycode+plninfo 来找到对应的
                        //代表ad_content_id是空的
                        if (citycode != null) {
                            String adcontent = city_pln_adcontent.get(extend.citycode + "," + extend.plninfo);
                            if (adcontent != null && !adcontent.equals("")) {
                                extend.setAdcontent(adcontent);
                            }
                        }
                    } else {
                        //根据adContentid来获取对应的adcontent
                        //获取对应的该条mac  观看的广告 adcontent
                        String adcontent = map_id_content.get(next.get(9).toString());
                        if (adcontent != null && !adcontent.equals("")) {
                            extend.setAdcontent(adcontent);
                        }
                    }

                    //用最大的批次号把为0 的给替换掉
                    if(extend.getSch_version()!=null && !extend.getSch_version().equals("0") &&
                            extend.getUploadcount().equals("0")
                            ){
                        String upload = city_version_uploadcount.get(extend.getCitycode() + "," + extend.getSch_version());
                        if(upload!=null && !upload.equals("")){
                            extend.setUploadcount(upload);
                        }
                    }

                    result_list.add(extend);
                }
                return result_list.iterator();
            }
        });

        Dataset<Row> extend_dataFrame = session.createDataFrame(macextend, MacViewExtend.class);
        extend_dataFrame.createOrReplaceTempView("tmp_mac_extend");

        //生成带有每个广告每天播放次数的mac_view_info
        Dataset<Row> mac_playnums = session.sql(" select m.*,p.play_nums from " +
                " (select *,concat(citycode,concat(sch_version,concat(uploadcount,adcontent))) as schedulekey from tmp_mac_extend) m " +
                " left join" +
                " (select *,concat(citycode,concat(buildinglist,concat(uploadcount,adcontent))) as schedulekey from tmp_result_adplay_times) p " +
                " on m.schedulekey=p.schedulekey " );
        mac_playnums.createOrReplaceTempView("tmp_mac_playnums");


        //开始计算60,120,240次等广告的人次和 人数

        //未去伪 人数   计算每个播放次数类型 的 扫描到的人数

        Dataset<Row> nofake_people_nums = session.sql("select t.city_name, t.play_nums, count(distinct(mac)) as nofake_nums,count(mac) as nofake_times from " +
                " (select * from tmp_mac_playnums where play_nums is not null) t" +
                " group by t.city_name, t.play_nums");
        nofake_people_nums.createOrReplaceTempView("tmp_nofake_result");
        /*//未去伪 人次  计算每个播放次数类型 的 人次
        Dataset<Row> nofake_people_times = session.sql("select t.city_name, t.play_nums, count(mac) from " +
                " (select * from tmp_mac_playnums where play_nums is not null) t" +
                " group by t.city_name, t.play_nums");*/


        ////////////////////////////////////////////////////////////
        //去伪 之后的人数 计算每个播放次数类型 的 扫描到的人数

        Dataset<Row> fake_people_nums = session.sql("select p.city_name, p.play_nums, count(distinct(mac)) as fake_nums,count(mac) as fake_times from " +
                " ( select t.* from " +
                "    (select *,substr(mac,0,6) as submac from tmp_mac_playnums where play_nums is not null ) t" +
                " left join" +
                " db01.mac_factory as p" +
                " on t.submac=p.mac_index_factory" +
                " where factory_name is not null " +
                " ) p" +
                " group by p.city_name, p.play_nums");
        fake_people_nums.createOrReplaceTempView("tmp_fake_result");
        /*//去伪之后 的人次
        Dataset<Row> fake_people_times = session.sql("select t.city_name, t.play_nums, count(mac) from " +
                " ( select t.* from " +
                "    (select *,substr(mac,0,6) as submac from tmp_mac_playnums where play_nums is not null ) t" +
                " left join" +
                " db01.mac_factory as p" +
                " on t.submac=p.mac_index_factory" +
                " where factory_name is not null " +
                " ) p" +
                " group by t.city_name, t.play_nums");*/

        Dataset<Row> day_fullflow  = session.sql("select '"+tdate+"' as day,n.*,m.fake_nums,m.fake_times from tmp_nofake_result  n" +
                " left join tmp_fake_result m" +
                " on n.city_name=m.city_name and n.play_nums=m.play_nums");
        day_fullflow.createOrReplaceTempView("tmp_result");
        session.sql("select * from tmp_result").show();
        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        day_fullflow.write().mode(SaveMode.Append).jdbc(URL,"ad_playtimes_day",pro);
    }

    public static void main(String[] args){

        String tdate=args[0];

        long tdate_long= DevDateUtil.dateStrToLong(tdate);
        String linedate = DevDateUtil.shadelongDateToStr(tdate_long);

        SparkSession session = JavaSparkUtil.getRemoteSparkSession("day_full_flow", "parquet_table");
        getFullFlowResult(session,tdate,linedate);
        session.stop();
    }
}
