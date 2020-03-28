package com.focusmedia.newgenerate;

import com.focusmedia.util.DevDateUtil;
import com.focusmedia.util.JavaSparkUtil;
import org.apache.avro.generic.GenericData;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;

/*
* 增补算法：
*
* 根据新的mac_scan_log数据格式来生成设备播放记录 和 增补合并之后的 mac 扫描记录
* */

//根据现在新的realtimeschedulescreenfam来生成播放表和macview表
//一个scch_evrsion中存在多个批次，如果在mac_scan_log中的批次中没有找到对应批次下的大小顺序号
//则就去最大批次号上面去找

public class GenerateNewMacScanInfo {

    //代表最大曝光的分割时间,毫秒数
    public static long MAX_SUB_TIME=10*60*60*1000L;
    public static long MAX_EXPOSE_TIME=5*60*1000L;
    //public static long END_PLAY_TIME=0;
    public static void main(String[] args){

        String tmp_devplay_table="tmp_devplay_info";
        String tmp_macview_table="tmp_macview_info";
        String appname=args[0];
        String tdate=args[1];
        String devid="";
        String city_name="";
        String table_suffix="";
        long must_append_time=0;    //代表往后增补多长时间
        long expose_flag_time=0;    //代表多长时间间隔才代表是两次曝光
        if(args.length>2){
            must_append_time=Integer.parseInt(args[2])*60*1000L;
        }

        if(args.length>3){
            expose_flag_time=Integer.parseInt(args[3])*60*1000L;
        }

        if(args.length>4){
            table_suffix=args[4];
            tmp_devplay_table=tmp_devplay_table+"_"+table_suffix;
            tmp_macview_table=tmp_macview_table+"_"+table_suffix;
        }

        if(args.length>5){
            city_name=args[5];
        }

        if(args.length>6){
            devid=args[6];
        }

        //用于生成最后补齐的截至第二天凌晨时间
        long tdate_long = DevDateUtil.dateStrToLong(tdate);
        //tdate2_long代表的是昨天的数据，只有日期没有时间
        long tdate2_long = tdate_long + 24 * 60 * 60 * 1000;
        //代表往上补到每天的早上6点
        long start_play_time= DevDateUtil.getSixHourLong(tdate);
        SparkConf conf=new SparkConf();
        conf.setAppName(appname);
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","1000");
        conf.set("spark.sql.crossJoin.enabled","true");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
        // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
        conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
        conf.set("spark.locality.wait","20s");
        conf.set("spark.network.timeout","500");
        conf.set("spark.sql.parquet.compression.codec", "gzip");
        conf.set("spark.sql.shuffle.partitions","1000");
        //conf.set("spark.speculation","true");
        //conf.set("spark.speculation.interval","6000ms");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        spark.sql("use parquet_table");

        //final Broadcast<SparkSession> spark_broadcast =context.broadcast(spark);
        //根据mac_view_info来生成对应的广告播放记录临时表 tmp_devplay_info
        //devid,start_time,start_time_long,end_time,end_time_long,sch_version,pls_index,
        //      play_index,isseed,p_length,ad_length,length_left,pln_info,seqkey,uploadcount
        getDevPLayInfo(spark, "parquet_table", tdate,tdate2_long,devid,tmp_devplay_table,city_name,start_play_time);

        one_getMacScaninfo(spark,tdate,must_append_time,expose_flag_time,devid,tmp_devplay_table,tmp_macview_table,city_name);

        spark.stop();
    }

    //第一版 生成mac 扫描记录, 并插入漏掉的，之后再次进行合并操作
    public static void one_getMacScaninfo(SparkSession session,String tdate,long must_append_time,long expose_flag_time,String devid,String tmp_devplay_table,String tmp_macview_table,String cityname){

        //从tmp_mac_view中获取原生的macview扫描记录
        JavaPairRDD<String, String> mac_view_pair = session.sql(
                " select m.* from ("+
                        " select devid,iccid,inner_sn,mac,sch_version,uploadcount,pls_index,play_index,plninfo, scan_time " +
                        " from edw.mac_view_info where time='" + tdate.replace("_","-") + "' " +
                        " and scan_time is not null  " +
                        " and (device_style like '%[智能互动]%' or device_style like '%[互动]%' )"+
                        " and (devid='"+devid+"' or ''='"+devid+"' ) " +
                        " and (city_name='"+cityname+"' or ''='"+cityname+"' )" +
                        " ) m left join db01.mac_factory f " +
                        "  on lower(substr(m.mac,1,6))=lower(f.mac_index_factory)" +
                        " where f.factory_name is not null ").toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                //根据设备和mac地址来进行分组
                return new Tuple2<String, String>(row.get(0).toString(), row.get(0).toString() + "\t" + row.get(1).toString() + "\t" +
                        row.get(2).toString() + "\t" + row.get(3).toString() + "\t" + row.get(4).toString() + "\t" + row.get(5).toString() + "\t" +
                        row.get(6).toString() + "\t" + row.get(7).toString() + "\t" + row.get(8).toString() + "\t" + row.get(9).toString()
                );
            }
        });

        // 获取相关设备播放数据
        JavaPairRDD<String, String> dev_play_pair = session.sql("select devid,start_time,start_time_long,end_time,end_time_long," +
                " sch_version,pls_index,play_index,isseed,p_length,ad_length,length_left,pln_info,seqkey,uploadcount" +
                " from " + tmp_devplay_table + " where time='" + tdate + "'").toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String,String>(row.get(0).toString(), row.get(0).toString()+"\t" + row.get(1).toString()+"\t"+
                         row.get(2).toString()+"\t"+ row.get(3).toString()+"\t"+ row.get(4).toString()+"\t"+ row.get(5).toString()+"\t"
                        + row.get(6).toString()+"\t"+ row.get(7).toString()+"\t"+ row.get(8).toString()+"\t"+ row.get(9).toString()+"\t"
                        + row.get(10).toString()+"\t"+ row.get(11).toString()+"\t"+ row.get(12).toString()+"\t"+ row.get(13).toString()+"\t"
                        + row.get(14).toString()+"\t");
            }
        });

        JavaPairRDD<String, String> play_and_macview = dev_play_pair.union(mac_view_pair);
        JavaRDD<MacViewBean> macViewBeanJavaRDD = play_and_macview.groupByKey().flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, MacViewBean>() {
            @Override
            public Iterator<MacViewBean> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                //JedisCluster instance = RedisUtils.getInstance();
                //获取设备id
                String devid = tuple2._1;
                //String devid_key="devid_"+devid;
                //获取当前设备下的扫描记录和播放记录
                Iterable<String> all_info = tuple2._2;

                //封装这个设备下的每个mac的扫描记录
                final Map<String, List<MacScanBean>> mac_scans = new HashMap<String, List<MacScanBean>>();

                //封装这个设备下的播放记录
                final List<RealPlayLog> dev_play=new ArrayList<>();
                //用来存储最终的进过合并或者插入的mac记录
                final List<MacViewBean> new_merge_mac_scan = new ArrayList<MacViewBean>();

                //用来保存对应inner_sn  和 该innersn下面所对应的扫描记录
                final Map<String,List<MacScanBean>> inner_macscan=new HashMap<String,List<MacScanBean>>();

                all_info.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        String[] cols = s.split("\t");

                        if (cols.length == 10) {
                            //代表是扫描记录
                            String mac = cols[3];
                            //devid,iccid,inner_sn,mac,sch_version,uploadcount,pls_index,play_index,plninfo, scan_time
                            long scan_time_long = DevDateUtil.timeStrToLong(cols[9]);
                            MacScanBean bean = new MacScanBean(cols[0], cols[1], cols[2], cols[3],
                                    cols[4], cols[5], cols[6], cols[7], cols[8], cols[9], scan_time_long,0
                            );
                            if(inner_macscan.get(bean.getInner_sn())==null){
                                List<MacScanBean> inner_scan=new ArrayList<MacScanBean>();
                                inner_scan.add(bean);
                                inner_macscan.put(bean.getInner_sn(),inner_scan);
                            }else{
                                inner_macscan.get(bean.getInner_sn()).add(bean);
                            }
                        } else if (cols.length == 15) {
                            //代表是设备播放机记录
                            //devid,start_time,start_time_long,end_time,end_time_long," +
                            //     " sch_version,pls_index,play_index,isseed,p_length,ad_length,length_left,pln_info,seqkey,uploadcount"
                            long start_time_long = Long.parseLong(cols[2]);
                            long end_time_long = Long.parseLong(cols[4]);
                            int sch_version = Integer.parseInt(cols[5]);
                            int pls_index = Integer.parseInt(cols[6]);
                            int play_idnex = Integer.parseInt(cols[7]);
                            boolean isseed = Boolean.parseBoolean(cols[8]);

                            long p_length = Long.parseLong(cols[9]);
                            long ad_length = Long.parseLong(cols[10]);
                            long length_left = Long.parseLong(cols[11]);

                            int uploadcount = Integer.parseInt(cols[14]);
                            RealPlayLog playlog = new RealPlayLog(cols[0], cols[1], start_time_long, cols[3], end_time_long,
                                    sch_version, pls_index, play_idnex, isseed, p_length, ad_length, length_left, cols[12], cols[13], uploadcount
                            );
                            dev_play.add(playlog);
                        }
                    }
                });
                //System.out.println("inner macscan的数据量："+inner_macscan.size());
                if(inner_macscan.size()==0){
                    return new_merge_mac_scan.iterator();
                }
                int max_size=0;
                String max_inner_sn="";
                for(Map.Entry<String,List<MacScanBean>> entry:inner_macscan.entrySet()){
                    if(entry.getValue().size()>max_size){
                        max_size=entry.getValue().size();
                        max_inner_sn=entry.getKey();
                    }
                }
                //System.out.println("选取出来的最多记录数的inner_sn是："+max_inner_sn);
                //获取inner_sn 对应最多的扫描记录,并且根据mac进行分割
                List<MacScanBean> innermacScanBeans= inner_macscan.get(max_inner_sn);
                List<String> del_macs=new ArrayList<>();
                for(MacScanBean bean:innermacScanBeans){
                    if(mac_scans.get(bean.getMac())==null && !del_macs.contains(bean.getMac())){
                        List<MacScanBean> sub_list=new ArrayList<MacScanBean>();
                        sub_list.add(bean);
                        mac_scans.put(bean.getMac(),sub_list);
                    }else{
                        if(mac_scans.get(bean.getMac()) !=null && mac_scans.get(bean.getMac()).size()>1000){
                            mac_scans.remove(bean.getMac());
                            del_macs.add(bean.getMac());
                        }else{
                            if(!del_macs.contains(bean.getMac())){
                                mac_scans.get(bean.getMac()).add(bean);
                            }
                        }
                    }
                }

                Map<String, Integer> stringIntegerMap = null;
                Map<String,List<RealPlayLog>> seqkey_playlog=new HashMap<String,List<RealPlayLog>>();
                //代表该设备的播放记录不为空，存在播放记录
                if (dev_play != null && dev_play.size() > 0) {
                    dev_play.sort(new Comparator<RealPlayLog>() {
                        @Override
                        public int compare(RealPlayLog o1, RealPlayLog o2) {
                            int starttime_compare = Long.compare(o1.start_time_long, o2.start_time_long);
                            if (starttime_compare == 0) {
                                int version_compare = Integer.compare(o1.sch_version, o2.sch_version);
                                if (version_compare == 0) {
                                    int upload_compare = Integer.compare(o1.uploadcount, o2.uploadcount);
                                    if (upload_compare == 0) {
                                        int pls_compare = Integer.compare(o1.pls_index, o2.pls_index);
                                        if (pls_compare == 0) {
                                            int play_compare = Integer.compare(o1.play_index, o2.play_index);
                                            return play_compare;
                                        } else {
                                            return pls_compare;
                                        }
                                    } else {
                                        return upload_compare;
                                    }
                                } else {
                                    return version_compare;
                                }
                            } else {
                                return starttime_compare;
                            }
                        }
                    });

                    stringIntegerMap = gendevPlayLogMap(dev_play);

                    for(RealPlayLog playlog:dev_play){
                        if(seqkey_playlog.get(playlog.seqkey)==null){
                            List<RealPlayLog> playLogList=new ArrayList<RealPlayLog>();
                            playLogList.add(playlog);
                            seqkey_playlog.put(playlog.seqkey,playLogList);
                        }else{
                            seqkey_playlog.get(playlog.seqkey).add(playlog);
                        }
                    }
                }

                //对当前设备的下的mac 扫描的所有扫描记录进行处理
                //System.out.println("设备："+devid_key+"处理开始时间"+new Date());
                for (Map.Entry<String, List<MacScanBean>> entry : mac_scans.entrySet()) {
                    String mac = entry.getKey();
                    //System.out.println("增补：对Mac"+mac+" 数据进行处理");
                    List<MacScanBean> mac_scan_list = entry.getValue();
                    if(mac_scan_list.size()>1000){
                        continue;
                    }
                   // System.out.println("设备："+devid+"下 Mac数据 处理开始"+mac+"|"+entry.getValue().size()+"|"+new Date());
                    mac_scan_list.sort(new Comparator<MacScanBean>() {
                        @Override
                        public int compare(MacScanBean o1, MacScanBean o2) {
                            return Long.compare(o1.scan_time_long, o2.scan_time_long);
                        }
                    });

                    List<MacScanBean> macScanBeans = mergeAndInsertScanLog(mac_scan_list, dev_play, stringIntegerMap, must_append_time,expose_flag_time);

                    //在对插入新的mac扫描记录之后的list进行合并处理，并生成end_time,end_time_long字段

                    List<MacViewBean> macViewBeans = combineMacScanBean(macScanBeans, seqkey_playlog,expose_flag_time);
                   // System.out.println("设备："+devid+"  mac "+mac+" | combineMacScanBean 方法完毕:"+new Date());
                    //System.out.println("增补: combineMacScanBean After");
                   // System.out.println("设备："+devid+"下 Mac数据 处理完毕"+mac+"|"+entry.getValue().size()+"|"+new Date());
                    new_merge_mac_scan.addAll(macViewBeans);
                    if(new_merge_mac_scan.size()>2000000){
                        System.out.println("该设备"+devid+"增补后的数据量超过了2000000");
                        //return new_merge_mac_scan.iterator();
                    }
                }
                //System.out.println("设备："+devid_key+"处理完毕时间"+new Date()+new_merge_mac_scan.size());
                /*if(new_merge_mac_scan.size()>=5000000){
                    System.out.println("设备："+devid_key+"处理完毕时间"+new Date()+"|"+new_merge_mac_scan.size());
                }*/
                return new_merge_mac_scan.iterator();
            }
        });

        Dataset<Row> macViewDataFrame = session.createDataFrame(macViewBeanJavaRDD, MacViewBean.class);
        macViewDataFrame.createOrReplaceTempView("tmp_macview");
        // session.sql("select count(*) from tmp_macview").show();

        //session.sql("select * from tmp_macview where mac='38a4edbe7839'").show(300);
        macViewDataFrame.repartition(100).write().mode(SaveMode.Overwrite).parquet("hdfs://ns1/parquet/macview/"+tdate);
        saveMacViewInfo(session,tmp_macview_table,tdate);
    }


    public static void saveMacViewInfo(SparkSession session,String macViewTable,String tdate){

        session.sql("CREATE TABLE IF NOT EXISTS "+macViewTable+" ( " +
                " devid string,iccid string,inner_sn string,mac string,sch_version string,uploadcount string," +
                " pls_index string,play_index string,plninfo string,start_time string,start_time_long long,end_time string," +
                " end_time_long long,time_diff long)" +
                " PARTITIONED BY (time string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
                " ");
        /*session.sql("insert overwrite table "+macViewTable+" partition(time='" + tdate + "') select " +
                " devid,iccid,inner_sn,mac,sch_version,uploadcount,pls_index,play_index,plninfo,start_time," +
                " start_time_long,end_time,end_time_long from tmp_macview ");*/
        session.sql("load data inpath '/parquet/macview/"+tdate+"/*.parquet' overwrite into table "+macViewTable+" partition (time='"+tdate+"')");

    }

    //对生成mac扫描记录进行合并处理
    private static List<MacViewBean> combineMacScanBean(List<MacScanBean> macScanBeans,Map<String,List<RealPlayLog>> seqkey_playlog,long expose_flag_time) {
        List<MacViewBean> mac_results=new ArrayList<MacViewBean>();
        //代表前一条 mac view扫描记录
        MacViewBean pre_mac_viewlog = null;
        int tmp_index=0;
        boolean flag=true;
        for (MacScanBean cur_exposelog : macScanBeans) {
            //System.out.println("Start Mac Data:"+cur_exposelog.toString()+" 开始合并处理");
            tmp_index++;
            if (pre_mac_viewlog != null) {
                //拿当前条和上一条进行对比scan_time_long时间的对比
                if (cur_exposelog.scan_time_long - pre_mac_viewlog.end_time_long > expose_flag_time) {
                    //代表了是不同的曝光,则直接生成新的mac扫描记录
                    long start_time_long = DevDateUtil.timeStrToLong(cur_exposelog.scan_time);
                    long end_time_long = start_time_long + 1L;
                    String end_time = DevDateUtil.longTimeToStr(end_time_long);
                    MacViewBean viewLog = new MacViewBean(cur_exposelog.devid, cur_exposelog.iccid, cur_exposelog.inner_sn,
                            cur_exposelog.mac,  cur_exposelog.sch_version,cur_exposelog.uploadcount, cur_exposelog.pls_index, cur_exposelog.play_index
                            ,cur_exposelog.plninfo, cur_exposelog.scan_time, start_time_long, end_time, end_time_long,cur_exposelog.time_diff);
                    mac_results.add(viewLog);
                    pre_mac_viewlog = viewLog;
                } else {
                    //代表是同一次曝光，需要将下一条的scan_time赋值给上一条的end_time，并且新增曝光记录

                    //接着判断两个紧挨着的是否同一个广告，如果是的直接合并
                    //如果出现了在一个循环中中途重启了，则重启后重新开始的也会创建为一个新的曝光记录
                    if (cur_exposelog.sch_version.equals(pre_mac_viewlog.sch_version) &&
                            cur_exposelog.uploadcount.equals(pre_mac_viewlog.uploadcount) &&
                            cur_exposelog.pls_index.equals(pre_mac_viewlog.pls_index) &&
                            cur_exposelog.play_index.equals(pre_mac_viewlog.play_index)) {
                        //代表是同一条广告的扫描记录，这个时候只需要更新上一条的时间不需要新增mac 曝光记录
                        pre_mac_viewlog.updateEndTime(cur_exposelog.scan_time_long);
                        flag=false;

                    } else {
                        //再将这一次曝光的不同广告记录新增进去
                        long start_time_long = DevDateUtil.timeStrToLong(cur_exposelog.scan_time);
                        long end_time_long = start_time_long + 1L;
                        String end_time = DevDateUtil.longTimeToStr(end_time_long);
                        MacViewBean viewLog = new MacViewBean(cur_exposelog.devid, cur_exposelog.iccid, cur_exposelog.inner_sn,
                                cur_exposelog.mac,  cur_exposelog.sch_version, cur_exposelog.uploadcount,cur_exposelog.pls_index, cur_exposelog.play_index
                                ,cur_exposelog.plninfo, cur_exposelog.scan_time, start_time_long, end_time, end_time_long,cur_exposelog.time_diff);
                        mac_results.add(viewLog);
                        pre_mac_viewlog = viewLog;
                    }
                }
            } else {
                //代表是该mac的第一条扫描记录，直接新增mac view log  记录
                long start_time_long = DevDateUtil.timeStrToLong(cur_exposelog.scan_time);
                long end_time_long = start_time_long + 1L;
                String end_time = DevDateUtil.longTimeToStr(end_time_long);
                MacViewBean viewLog = new MacViewBean(cur_exposelog.devid, cur_exposelog.iccid, cur_exposelog.inner_sn,
                        cur_exposelog.mac,  cur_exposelog.sch_version, cur_exposelog.uploadcount,cur_exposelog.pls_index, cur_exposelog.play_index
                        ,cur_exposelog.plninfo, cur_exposelog.scan_time, start_time_long, end_time, end_time_long,cur_exposelog.time_diff);
                mac_results.add(viewLog);
                pre_mac_viewlog = viewLog;
            }
            flag=true;
            //System.out.println(" END Mac Data:"+cur_exposelog.toString()+" 结束合并处理");
        }
        return mac_results;
    }


    //获取macscan 和 设备对应的排片信息
    public static void getDevPLayInfo(SparkSession session, String dblocation, String tdate,long end_play_time,String devid,String dev_table_name,String cityname,long start_play_time)
    {
        long tdate_long = DevDateUtil.dateStrToLong(tdate);
        String shade_date= DevDateUtil.shadelongDateToStr(tdate_long);

        //获取当天的mac_view_info全量数据
        //与后面增补的取数逻辑一模一样
        Dataset<Row> mac_view_info = session.sql("select m.* from (" +
                " select devid,sch_version,uploadcount,pls_index,play_index,plninfo," +
                " scan_time,citycode,ad_content_id,inner_sn,mac" +
                " from edw.mac_view_info where time='"+tdate.replace("_","-")+"' " +
                " and (device_style like '%[智能互动]%' or device_style like '%[互动]%' ) " +
                " and scan_time is not null " +
                " and (devid='"+devid+"' or ''='"+devid+"' ) " +
                " and (city_name='"+cityname+"' or ''='"+cityname+"' )  " +
                " ) m left join db01.mac_factory f " +
                " on  lower(substr(m.mac,1,6))=lower(f.mac_index_factory)" +
                " where f.factory_name is not null " +
                "");

       /* Dataset<Row> mac_view_info = session.sql("select devid,sch_version,uploadcount,pls_index,play_index,plninfo," +
                " scan_time,citycode,ad_content_id" +
                " from mac_view_info_mrdtest");*/

        mac_view_info.createOrReplaceTempView("tmp_mac_view");


        //获取去重后的全量排期数据
        //sch_version,uploadcount,pls_index,play_index,adlength,plnid
        Dataset<Row> total_schedule = session.sql("select buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno," +
                " nplinnerseqno,uploadcount,adlength " +
                " from realtimeschedulescreenfam_by_day where buildinglistdate='" +shade_date+"' "+
                " group by buildinglist,buildinglistdate,citycode,plnid,adcontent,nplseqno,nplinnerseqno,uploadcount,adlength");
        total_schedule.createOrReplaceTempView("tmp_real_schedule");

        //排期数据量不大，所以直接收集到客户端
        List<Row> rows = total_schedule.collectAsList();
        final Map<String,List<RealScheduleBean>> city_schedule=new HashMap<String,List<RealScheduleBean>>();
        for(Row row:rows){

            RealScheduleBean schedule=new RealScheduleBean();
            schedule.setBuildinglist(row.get(0).toString());
            schedule.setBuildinglistdate(row.get(1).toString());
            schedule.setCitycode(row.get(2).toString());
            schedule.setPlnid(row.get(3).toString());
            schedule.setAdcontent(row.get(4).toString());
            schedule.setNplseqno(row.get(5).toString());
            schedule.setNplinnerseqno(row.get(6).toString());
            schedule.setUploadcount(row.get(7).toString());
            schedule.setAdlength(row.get(8).toString());

            if(!row.isNullAt(2) &&city_schedule.get(row.get(2).toString())==null){
                List<RealScheduleBean> schedule_list=new ArrayList<RealScheduleBean>();
                schedule_list.add(schedule);
                city_schedule.put(row.get(2).toString(),schedule_list);
            }else{
                city_schedule.get(row.get(2).toString()).add(schedule);
            }
        }

        //根据mac_view_info中的数据和排期数据生成对应的每个设备的播放记录数据

        JavaRDD<RealPlayLog> realPlayLogJavaRDD = mac_view_info.toJavaRDD().mapToPair(new PairFunction<Row, String, RealDevScanLogBean>() {

            @Override
            public Tuple2<String, RealDevScanLogBean> call(Row row) throws Exception {

                //devid,sch_version,uploadcount,pls_index,play_index,plninfo," +
                //                " scan_time,citycode,ad_content_id"
                String citycode=row.isNullAt(7)?null:row.get(7).toString();
                String ad_content_id=row.isNullAt(8)?null:row.get(8).toString();
                String inner_sn=row.isNullAt(9)?null:row.get(9).toString();
                String sch_version=row.isNullAt(1)?null:row.get(1).toString();
                String uploadcount=row.isNullAt(2)?null:row.get(2).toString();
                String pls_index=row.isNullAt(3)?null:row.get(3).toString();
                String play_index=row.isNullAt(4)?null:row.get(4).toString();
                String plninfo=row.isNullAt(5)?null:row.get(5).toString();
                if(sch_version==null || uploadcount==null ||pls_index==null||play_index==null||
                        plninfo==null
                        ){
                    System.out.println("有数据sv_uploadcount_pls_play_plninfo为空:"+row.toString());
                }
                return new Tuple2<String, RealDevScanLogBean>(row.get(0).toString(), new RealDevScanLogBean(row.get(0).toString(),
                        sch_version, uploadcount, pls_index, play_index,
                        plninfo, row.get(6).toString(), citycode,ad_content_id,
                        DevDateUtil.timeStrToLong(row.get(6).toString()), sch_version + "," + uploadcount + "," +
                        pls_index + "," + play_index+","+plninfo,inner_sn
                ));
            }
        }).groupByKey().flatMap(new FlatMapFunction<Tuple2<String, Iterable<RealDevScanLogBean>>, RealPlayLog>() {
            @Override
            public Iterator<RealPlayLog> call(Tuple2<String, Iterable<RealDevScanLogBean>> devid_macrow) throws Exception {

                String devid = devid_macrow._1;
                Map<String,List<RealDevScanLogBean>> sn_listscan=new HashMap<>();
                Iterable<RealDevScanLogBean> realDevScanLogBeans = devid_macrow._2;

                realDevScanLogBeans.forEach(new Consumer<RealDevScanLogBean>() {
                    @Override
                    public void accept(RealDevScanLogBean realDevScanLogBean) {
                        String inner_sn = realDevScanLogBean.getInner_sn();
                        if(sn_listscan.get(inner_sn)==null){
                            List<RealDevScanLogBean> reals=new ArrayList<>();
                            reals.add(realDevScanLogBean);
                            sn_listscan.put(inner_sn,reals);
                        }else{
                            sn_listscan.get(inner_sn).add(realDevScanLogBean);
                        }
                    }
                });

                //比较那个inner_sn下面的扫描记录最多
                int max_size=0;
                String max_innersn="";
                for(Map.Entry<String,List<RealDevScanLogBean>> entry:sn_listscan.entrySet()){
                    String inner_sn = entry.getKey();
                    int cur_size=entry.getValue().size();
                    if(cur_size>max_size){
                        max_size=cur_size;
                        max_innersn=inner_sn;
                    }
                }

                //只取那个inner_sn 下面扫描数最多的那个来生成设备播放表
                final List<RealDevScanLogBean> allscan_log = sn_listscan.get(max_innersn);
                //按照时间和版本号，批次号，大小顺序号来进行升序排序
                allscan_log.sort(new Comparator<RealDevScanLogBean>() {
                    @Override
                    public int compare(RealDevScanLogBean bean1, RealDevScanLogBean bean2) {
                        int scan_time_compare = Long.compare(bean1.getScan_time_long(), bean2.getScan_time_long());
                        if (scan_time_compare == 0) {
                            int version_compare = Integer.compare(Integer.parseInt(bean1.getSch_version()), Integer.parseInt(bean2.getSch_version()));
                            if (version_compare == 0) {
                                int uploadcount_compare = Integer.compare(Integer.parseInt(bean1.getUploadcount()), Integer.parseInt(bean2.getUploadcount()));
                                if (uploadcount_compare == 0) {
                                    int pls_compare = Integer.compare(Integer.parseInt(bean1.getPls_index()), Integer.parseInt(bean2.getPls_index()));
                                    if (pls_compare == 0) {
                                        int play_compare = Integer.compare(Integer.parseInt(bean1.getPlay_index()), Integer.parseInt(bean2.getPlay_index()));
                                        return play_compare;
                                    } else {
                                        return pls_compare;
                                    }
                                } else {
                                    return uploadcount_compare;
                                }
                            } else {
                                return version_compare;
                            }
                        } else {
                            return scan_time_compare;
                        }
                    }
                });
                List<RealPlayLog> realPlayLogs=new ArrayList<RealPlayLog>();
                //随机选取一条 设备 扫描数据，获取对应城市,然后根据城市获取排期数据
                String citycode = allscan_log.get(0).getCitycode();
                if(citycode==null){
                    return realPlayLogs.iterator();
                }
                List<RealScheduleBean> realScheduleBeans = city_schedule.get(citycode);
                if(realScheduleBeans==null || realScheduleBeans.size()==0){
                    return realPlayLogs.iterator();
                }
                Map<String, List<RealScheduleBean>> verUpload_schedule = new HashMap<String, List<RealScheduleBean>>();
                for (RealScheduleBean bean : realScheduleBeans) {
                    //版本 +,+ 批次 组合成键
                    String sch_version_uploadcount = bean.getBuildinglist() + "," + bean.getUploadcount();

                    if (verUpload_schedule.get(sch_version_uploadcount) == null) {
                        List<RealScheduleBean> sch_list = new ArrayList<RealScheduleBean>();
                        sch_list.add(bean);
                        verUpload_schedule.put(sch_version_uploadcount, sch_list);
                    } else {
                        verUpload_schedule.get(sch_version_uploadcount).add(bean);
                    }
                }

                //用来存储对应广告的长度
                HashMap<String, Long> ad_length_map = new HashMap<String, Long>();

                HashMap<String, String> ad_plnid_map = new HashMap<String, String>();

                //用来存储某个版本+批次 下的 每个广告的下一个广告顺序
                HashMap<String, HashMap<String, String>> sch_ad_next = new HashMap<String, HashMap<String, String>>();


                //用来存储某个版本+批次 下的 每个广告的上一个广告顺序
                HashMap<String, HashMap<String, String>> sch_ad_pre = new HashMap<String, HashMap<String, String>>();

                //用于存储当前版本和批次 下的广告总长度
                Map<String, Long> ad_sumlength_map = new HashMap<String, Long>();

                //对这个城市里面的版本和批次下的 大小顺序号进行升序排序
                //排序之后才能计算这个版本和批次里面的广告播放顺序，才能得知广告是不是挨着的
                for (Map.Entry<String, List<RealScheduleBean>> entry : verUpload_schedule.entrySet()) {

                    //用来存储当前广告 和下一条广告的对应关系
                    HashMap<String, String> ad_next_map = new HashMap<String, String>();
                    //用来存储当前广告 和上一条广告的对应关系
                    HashMap<String, String> ad_pre_map = new HashMap<String, String>();

                    String schVersion_uploadcount = entry.getKey();
                    entry.getValue().sort(new Comparator<RealScheduleBean>() {
                        @Override
                        public int compare(RealScheduleBean o1, RealScheduleBean o2) {
                            int pls_compare = Integer.compare(Integer.parseInt(o1.getNplseqno()), Integer.parseInt(o2.getNplseqno()));
                            if (pls_compare == 0) {
                                int play_compare = Integer.compare(Integer.parseInt(o1.getNplinnerseqno()), Integer.parseInt(o2.getNplinnerseqno()));
                                return play_compare;
                            } else {
                                return pls_compare;
                            }
                        }
                    });
                    List<RealScheduleBean> sub_schedule = entry.getValue();
                    RealScheduleBean first_schedule = null;
                    RealScheduleBean next_schedule = null;
                    int tmp_index = 0;
                    long sumlength = 0;
                    for (RealScheduleBean current_schedule : sub_schedule) {

                        //保存排期的plnid
                        ad_plnid_map.put(current_schedule.getBuildinglist() + "," + current_schedule.getUploadcount() + "," +
                                current_schedule.getNplseqno() + "," + current_schedule.getNplinnerseqno()+","+current_schedule.getPlnid(), current_schedule.getPlnid());

                        //这里的时长保存的是毫秒级别
                        ad_length_map.put(current_schedule.getBuildinglist() + "," + current_schedule.getUploadcount() + "," +
                                current_schedule.getNplseqno() + "," + current_schedule.getNplinnerseqno()+","+current_schedule.getPlnid(), (long) Double.parseDouble(current_schedule.getAdlength()) * 1000L
                        );
                        tmp_index++;
                        if (first_schedule == null) {
                            first_schedule = current_schedule;
                        }
                        if (tmp_index <= sub_schedule.size() - 1) {
                            next_schedule = sub_schedule.get(tmp_index);
                            ad_next_map.put(current_schedule.getBuildinglist() + "," + current_schedule.getUploadcount() + "," +
                                            current_schedule.getNplseqno() + "," + current_schedule.getNplinnerseqno()+","+current_schedule.getPlnid(),
                                    next_schedule.getBuildinglist() + "," + next_schedule.getUploadcount() + "," +
                                            next_schedule.getNplseqno() + "," + next_schedule.getNplinnerseqno()+","+next_schedule.getPlnid()
                            );
                        } else {
                            //代表当前排期的最后一条
                            ad_next_map.put(current_schedule.getBuildinglist() + "," + current_schedule.getUploadcount() + "," +
                                            current_schedule.getNplseqno() + "," + current_schedule.getNplinnerseqno()+","+current_schedule.getPlnid(),
                                    first_schedule.getBuildinglist() + "," + first_schedule.getUploadcount() + "," +
                                            first_schedule.getNplseqno() + "," + first_schedule.getNplinnerseqno()+","+first_schedule.getPlnid());
                            first_schedule = null;
                        }
                        sumlength += (long) Double.parseDouble(current_schedule.getAdlength()) * 1000;
                    }
                    //保存某个版本+ 批次下的广告总长度
                    ad_sumlength_map.put(schVersion_uploadcount, sumlength);
                    sch_ad_next.put(schVersion_uploadcount, ad_next_map);


                    //用来构建每个版本,批次下 当前广告对应的上一条广告的对应关系
                    int pre_index=sub_schedule.size()-1;
                    RealScheduleBean last_schedule = null;
                    RealScheduleBean last_pre_schedule = null;
                    for(int i=pre_index;i>=0;i--){

                        RealScheduleBean currentbean = sub_schedule.get(i);
                        if(last_schedule==null){
                            last_schedule=currentbean;
                        }
                        if(i-1>=0){
                            //代表还没到第一条
                            last_pre_schedule=sub_schedule.get(i-1);
                            ad_pre_map.put(currentbean.getBuildinglist()+","+currentbean.getUploadcount()+","+
                            currentbean.getNplseqno()+","+currentbean.getNplinnerseqno()+","+currentbean.getPlnid(),
                                    last_pre_schedule.getBuildinglist()+","+last_pre_schedule.getUploadcount()+","+
                            last_pre_schedule.getNplseqno()+","+last_pre_schedule.getNplinnerseqno()+","+last_pre_schedule.getPlnid());
                        }else{
                            //代表已经到了第一条了
                            ad_pre_map.put(currentbean.getBuildinglist()+","+currentbean.getUploadcount()+","+
                                    currentbean.getNplseqno()+","+currentbean.getNplinnerseqno()+","+currentbean.getPlnid(),
                                    last_schedule.getBuildinglist()+","+last_schedule.getUploadcount()+","+last_schedule.getNplseqno()+","+
                            last_schedule.getNplinnerseqno()+","+last_schedule.getPlnid());
                            last_schedule=null;
                        }
                    }

                    sch_ad_pre.put(schVersion_uploadcount,ad_pre_map);
                }


                //准备开始生成这个设备的播放记录
                List<RealPlayLog> play_log_buf = new ArrayList<RealPlayLog>();

                //生成广告记录
                generateRealPlayLog(play_log_buf, allscan_log, ad_length_map);

                ////////////////////////////打印测试
               /* for(int i=0;i<play_log_buf.size();i++){
                    System.out.println(play_log_buf.get(i).toString());
                }*/


                //开始循环对有seed的数据进行上下更新时间
                boolean flag = true;
                while (flag) {
                    flag = changePlayLogTimeBySeed(play_log_buf, sch_ad_next, ad_sumlength_map);
                }

                //开始不断的扩展每一条的时间，直到没有数据时间可以扩展为止
                makeNoSeedChangeSeed(play_log_buf);

                //开始插入遗失的那些播放记录
                //开始补齐中间缺失的播放记录，补齐到下一条
                realPlayLogs = insertLostPlayLog(devid, play_log_buf, ad_length_map, ad_plnid_map, sch_ad_next, end_play_time);


                //根据第一条播放数据开始往上面补，最终补到每天的6点钟
                //System.out.println("开始准备往上补"+start_play_time);
                completedPrePlayLog(devid,realPlayLogs,ad_length_map, ad_plnid_map,sch_ad_pre,start_play_time);

                return realPlayLogs.iterator();
            }

        });

        /////////////////////////////////新增的代码,将设备播放数据保存到redis中
        /*realPlayLogJavaRDD.foreachPartition(new VoidFunction<Iterator<RealPlayLog>>() {
            @Override
            public void call(Iterator<RealPlayLog> realPlayLogIterator) throws Exception {
                String pre_devid=null;
                JedisCluster instance =RedisUtils.getInstance();
                Pipeline pipelined=null;
                Jedis redis=null;
                while(realPlayLogIterator.hasNext()){
                    RealPlayLog playlog = realPlayLogIterator.next();
                    String devid_key="devid_"+playlog.getDevid();

                    if(pre_devid==null){
                        String hostport = RedisPipelineUtil.getKeysSlots(devid_key, RedisPipelineUtil.slotHostMap);
                        redis=new Jedis(hostport.split(":")[0],Integer.parseInt(hostport.split(":")[1]),600000);
                        pipelined = redis.pipelined();
                        pipelined.lpush(devid_key,playlog.toString());
                    }else{

                        if(pre_devid.equals(playlog.getDevid())){
                            //如果当前条和上一条一样，则继续添加进去
                            pipelined.lpush(devid_key,playlog.toString());
                        }else{
                            //如果当前条和上一条不一样，则先sync
                            pipelined.sync();
                            pipelined.close();
                            redis.close();
                            String hostport = RedisPipelineUtil.getKeysSlots(devid_key, RedisPipelineUtil.slotHostMap);
                            redis=new Jedis(hostport.split(":")[0],Integer.parseInt(hostport.split(":")[1]),600000);
                            pipelined = redis.pipelined();
                            pipelined.lpush(devid_key,playlog.toString());
                        }
                    }
                    pre_devid=playlog.getDevid();
                }
                if(pipelined!=null){
                    pipelined.sync();
                    pipelined.close();
                }
                if(redis!=null && redis.isConnected()){
                    redis.close();
                }
                //instance.close();
            }
        });*/


        Dataset<Row> dataFrame = session.createDataFrame(realPlayLogJavaRDD, RealPlayLog.class);
        dataFrame.createOrReplaceTempView("tmp_devplay_info");


        dataFrame.repartition(100).write().mode(SaveMode.Overwrite).parquet("hdfs://ns1/parquet/devplay/"+tdate);
        //session.sql("select * from tmp_devplay_info").show(1000);
        saveDevPlayInfoToHive(session,tdate,"parquet_table",dev_table_name);
    }

    //往上补齐漏掉的广告信息
    private static void completedPrePlayLog(String devid,List<RealPlayLog> realPlayLogs, HashMap<String, Long> ad_length_map, HashMap<String, String> ad_plnid_map, HashMap<String, HashMap<String, String>> sch_ad_pre, long start_play_time) {

        if(realPlayLogs.size()>0){

            //获取第一条播放广告
            RealPlayLog first_realPlayLog = realPlayLogs.get(0);
            RealPlayLog pre_first_realPlayLog = first_realPlayLog;

            String pre_seqkey = sch_ad_pre.get(first_realPlayLog.sch_version + "," + first_realPlayLog.getUploadcount()).get(first_realPlayLog.getSeqkey());
            //System.out.println("第一次的当前播放记录:"+pre_first_realPlayLog.toString());
            //System.out.println("第一次 上一条seqkey为："+pre_seqkey);
            //获取上一条的广告长度
            long aLong = ad_length_map.get(pre_seqkey);
            //System.out.println("第一次获取的长度是:"+aLong);
            //只要往上补的时间小于start_time，就一直补
            while(pre_first_realPlayLog.getStart_time_long()-aLong>=start_play_time){
                //System.out.println("往上进来了+++++++++++++++++++++");
                //获取上一条的播放信息
                RealPlayLog insert_play_log = new RealPlayLog(devid,pre_seqkey,
                        pre_first_realPlayLog.start_time_long,ad_length_map.get(pre_seqkey),
                        ad_plnid_map.get(pre_seqkey),""
                );
                //把新增加的当作current
                pre_first_realPlayLog=insert_play_log;
                //获取上一条的seqkey
                pre_seqkey=sch_ad_pre.get(pre_first_realPlayLog.getSch_version()+","+pre_first_realPlayLog.getUploadcount()).get(pre_first_realPlayLog.getSeqkey());
                aLong = ad_length_map.get(pre_seqkey);

                realPlayLogs.add(0,insert_play_log);
                //System.out.println("新插入的记录:"+pre_first_realPlayLog.toString());
            }
        }
    }


    /*用来保存生成的播放记录数据*/
    public static  void saveDevPlayInfoToHive (SparkSession session, String tdate,String dblocation,String dev_playlog_table_name){

        session.sql("CREATE TABLE IF NOT EXISTS "+dev_playlog_table_name+" (" +
                " devid string," +
                " start_time string," +
                " start_time_long long," +
                " end_time string," +
                " end_time_long long," +
                " sch_version int," +
                " pls_index int," +
                " play_index int," +
                " isseed boolean," +
                " p_length long," +
                " ad_length long," +
                " length_left long," +
                " pln_info String," +
                " seqkey string," +
                " uploadcount int) " +
                " PARTITIONED BY (time string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' ");

       /* session.sql("insert overwrite table "+dev_playlog_table_name+" partition(time='" + tdate +
                "') select devid,start_time,start_time_long,end_time,end_time_long,sch_version, pls_index, play_index,isseed,p_length,ad_length,length_left,pln_info,seqkey,uploadcount from tmp_devplay_info");
*/
        session.sql(" load data inpath '/parquet/devplay/"+tdate+"/*.parquet' overwrite into table "+dev_playlog_table_name+" partition (time='"+tdate+"')");
    }

    //用于生成最后补齐之后的播放记录
    public static List<RealPlayLog> insertLostPlayLog(String devid,List<RealPlayLog> play_log_buf,HashMap<String,Long>  ad_length_map, Map<String,String> ad_plnid_map,HashMap<String, HashMap<String, String>> next_ad,long end_play_time){

        //新的补足中间后的播放数据
        List<RealPlayLog> new_playlog_list=new ArrayList<RealPlayLog>();
        //logger.info("=========================================================================="+max_sub_time);
        int log_index=0;
        RealPlayLog pre_new_playlog=null;
        //1,2,3,4,5,11......
        for(RealPlayLog current:play_log_buf){

            //先把当前的这条播放记录先插进去
            new_playlog_list.add(current);
            pre_new_playlog=current;
            log_index+=1;
            //logger.info("补齐:Current playlog is :"+current.toString());
            if(log_index<play_log_buf.size()) {
                //代表存在下一条
                RealPlayLog next_playlog = play_log_buf.get(log_index);
                //logger.info("补齐:Next playlog is :"+next_playlog.toString());

                String cur_verUp=current.getSeqkey().substring(0,current.getSeqkey().indexOf(",",current.seqkey.indexOf(",")+1));
                String next_verUp=next_playlog.getSeqkey().substring(0,next_playlog.getSeqkey().indexOf(",",next_playlog.seqkey.indexOf(",")+1));
                //检查是否属于同一个版本,同一批次的
                if (cur_verUp.equals(next_verUp)) {
                    //代表当前条和下一条 的 版本和批次一样，补齐sch_version,和批次号 相同的
                    if (current.isseed) {
                        //先往下进行补齐
                        long sub_time = next_playlog.start_time_long - current.end_time_long;
                        //logger.info("当前是seed补齐：Same Version NextStart-CurStart:"+sub_time);
                        if (sub_time < MAX_SUB_TIME) {
                            //小于该阈值时间才进行补
                            //不管下一条是否是同一条广告还是不同的、或者重启的，只要一直往下扩，扩到这个时间到达下一条的开始时间为止
                            //获取下一条的广告长度
                            long next_ad_length = ad_length_map.get(getNextAdSeqkey(current.seqkey,next_ad));
                            //logger.info("补齐 Normal next ad length is:"+next_ad_length);
                            //将当前条的结束时间加上下一条的广告长度
                            long perhap_next_starttime = current.end_time_long + next_ad_length;
                            //logger.info("补齐 After add the NextStartTime is:"+perhap_next_starttime);
                            String perhap_time_str= DevDateUtil.longTimeToStr(perhap_next_starttime);

                            //补齐到到下一条的开始时间
                            //如果最后的时间比下一条大了1秒之内，并且转化为时间格式后相等，则可以添加
                            while (perhap_next_starttime <= next_playlog.start_time_long ||(perhap_next_starttime>next_playlog.start_time_long && perhap_next_starttime-next_playlog.start_time_long<1000L && perhap_time_str.equals(next_playlog.start_time))) {

                                //代表当前条的结束时间加上下一条的广告之后依然小于下一条的广告播放时间，则往下扩展

                                String next_seqkey=getNextAdSeqkey(pre_new_playlog.getSeqkey(),next_ad);
                                RealPlayLog insert_play_log = new RealPlayLog(devid,next_seqkey,
                                        pre_new_playlog.end_time_long,ad_length_map.get(next_seqkey),
                                        ad_plnid_map.get(next_seqkey)
                                        );

                                //将新生成的广告记录，添加的播放记录里面去
                                new_playlog_list.add(insert_play_log);
                                //logger.info("插入了一条漏掉的广告播放机记录:"+insert_play_log.toString());
                                //将新生成的广告记录当作上一条的广告记录，也就是说1,2,3,4,5,6...11,
                                // 6 会当作新的上一条广告记录
                                pre_new_playlog = insert_play_log;

                                next_ad_length = ad_length_map.get(getNextAdSeqkey(pre_new_playlog.getSeqkey(),next_ad));
                                //logger.info("下一条广告的长度是："+next_ad_length);
                                perhap_next_starttime = pre_new_playlog.end_time_long + next_ad_length;
                                perhap_time_str= DevDateUtil.longTimeToStr(perhap_next_starttime);
                                //logger.info("下一跳的开始时间："+perhap_next_starttime);
                            }
                        }
                    }
                } else {
                    //代表当前条和下一条的版本号+批次号不一样  ,直接将当前版本往下扩max_sub_time段时间
                    //  一直到达到下一条的开始时间为止
                    if (current.isseed) {
                        //取一半的时间
                        long half_sub_time = MAX_SUB_TIME;

                        //获取预期的总的扩展到达时间
                        long maybe_last_time = pre_new_playlog.getEnd_time_long() + half_sub_time;
                        //logger.info("补齐 Not Same Version 预期扩展时间："+maybe_last_time);
                        //实际的扩展到达时间
                        long fact_last_time = maybe_last_time > next_playlog.start_time_long ? next_playlog.start_time_long : maybe_last_time;
                        //logger.info("补齐 Not Same Version 实际扩展时间："+fact_last_time);
                        //获取下一条的广告长度
                        long next_ad_length = ad_length_map.get(getNextAdSeqkey(pre_new_playlog.getSeqkey(),next_ad));

                        //logger.info("补齐 Not Same Version 下一条广告的长度："+next_ad_length);

                        while (pre_new_playlog.end_time_long + next_ad_length <= fact_last_time) {
                            //代表可以往下扩展
                            //代表当前条的结束时间加上下一条的广告之后依然小于下一条的广告播放时间，则往下扩展
                            String next_seqkey=getNextAdSeqkey(pre_new_playlog.getSeqkey(),next_ad);

                            RealPlayLog insert_play_log = new RealPlayLog(devid,next_seqkey,
                                    pre_new_playlog.end_time_long,ad_length_map.get(next_seqkey),
                                    ad_plnid_map.get(next_seqkey)
                            );
                            //将新生成的广告记录，添加的播放记录里面去
                            new_playlog_list.add(insert_play_log);
                            pre_new_playlog = insert_play_log;
                            next_ad_length = ad_length_map.get(getNextAdSeqkey(current.seqkey,next_ad));
                            //logger.info("补齐 insert not same version playlog:"+insert_play_log);
                        }
                    }
                }
            }else{

                //代表的是最后一条,则直接往下扩展max_sub_time/2段时间

                if(pre_new_playlog.getEnd_time_long()<end_play_time){
                    //取一半的时间
                    long half_sub_time = MAX_SUB_TIME ;

                    //获取预期的总的扩展到达时间,补到第二天的凌晨两点为止
                    long maybe_last_time = pre_new_playlog.getEnd_time_long() + half_sub_time <end_play_time?pre_new_playlog.getEnd_time_long() + half_sub_time:end_play_time;
                    //logger.info("补齐 Not Same Version 预期扩展时间："+maybe_last_time);

                    //获取下一条的广告长度
                    long next_ad_length = ad_length_map.get(getNextAdSeqkey(pre_new_playlog.getSeqkey(),next_ad));

                    //logger.info("补齐 Not Same Version 下一条广告的长度："+next_ad_length);

                    while (pre_new_playlog.end_time_long + next_ad_length <= maybe_last_time) {
                        //代表可以往下扩展
                        //代表当前条的结束时间加上下一条的广告之后依然小于下一条的广告播放时间，则往下扩展

                        String next_seqkey=getNextAdSeqkey(pre_new_playlog.getSeqkey(),next_ad);

                        RealPlayLog insert_play_log = new RealPlayLog(devid,next_seqkey,
                                pre_new_playlog.end_time_long,ad_length_map.get(next_seqkey),
                                ad_plnid_map.get(next_seqkey)
                        );
                        //将新生成的广告记录，添加的播放记录里面去
                        new_playlog_list.add(insert_play_log);
                        pre_new_playlog = insert_play_log;
                        next_ad_length = ad_length_map.get(getNextAdSeqkey(pre_new_playlog.seqkey,next_ad));
                        //logger.info("补齐 insert not same version playlog:"+insert_play_log);
                    }
                }
            }
        }
        return new_playlog_list;
    }

    //获取下一条广告的 版本+批次号+文件顺序号+广告播放顺序号
    public static String getNextAdSeqkey(String cur_seqkey,HashMap<String, HashMap<String, String>> next_ad){

        String verUp=cur_seqkey.substring(0,cur_seqkey.indexOf(",",cur_seqkey.indexOf(",")+1));
        //获取这个版本+批次下面的所有 的大小顺序号的排期信息
        HashMap<String, String> pls_play_map = next_ad.get(verUp);

        return pls_play_map.get(cur_seqkey);
    }
    
    //用于进行seed扩展，分别往左扩展和往右扩展
    public static void makeNoSeedChangeSeed(List<RealPlayLog> play_log_buf){

        RealPlayLog pre_playlog=null;
        RealPlayLog next_playlog=null;
        int log_index=0;
        long flag=1;
        while(flag>0){
            //第一次开始循环的时候，将flag设置为0，因为不知道有没有能扩展的记录，以防死循环
            flag=0;
            //每次开始一次新的循环时，pre_playlog都改为空的
            pre_playlog=null;
            log_index=0;
            for(RealPlayLog current:play_log_buf){
                log_index+=1;
                if(log_index<play_log_buf.size()){
                    next_playlog=play_log_buf.get(log_index);
                }
                if(!current.isseed){

                    //先进行左扩展看能不能扩
                    if(extendLeft(current,pre_playlog,200L)){
                        //代表左边可以进行扩，所以flag++
                        flag++;
                        if(!current.isseed){
                            //如果不是seed再接着扩展右边的
                            if(extendRight(current,next_playlog,200L)){
                                //代表右边也可以扩，所以flag++，然后直接下一条
                                flag++;
                            }
                        }
                    }else{
                        //左不能扩了，但是右还可以扩展
                        if(extendRight(current,next_playlog,200L)){
                            //代表右边可以进行扩展，所以flag++
                            flag++;
                        }
                    }
                }
                pre_playlog=current;
            }
        }
    }

    //扩展左边的开始时间
    public static boolean  extendLeft(RealPlayLog current,  RealPlayLog pre, long i){
        boolean rs = false;
        boolean log = false;
        if (pre == null) {
            current.extendstart(i);
            rs = true;
        } else {
            if (pre.end_time_long >= current.start_time_long) {
                rs = false;
            } else  {
                long subtm = current.start_time_long - pre.end_time_long;
                if (subtm < i) {
                    current.extendstart(subtm);
                } else {
                    current.extendstart(i);
                }
                rs = true;
            }
        }
        return rs;
    }

    //扩展右边的开始时间
    public static boolean extendRight(RealPlayLog current, RealPlayLog next, long extend_time){
        boolean rs = false;
        if (next == null) {
            current.extendend(extend_time);
            rs = true;
        } else {
            if (next.start_time_long <= current.end_time_long) {
                rs = false;
            } else {
                long subtm = next.start_time_long - current.end_time_long;
                if (subtm < extend_time) {
                    current.extendend(subtm);
                } else {
                    current.extendend(extend_time);
                }
                rs = true;
            }
        }
        return rs;
    }

    //检查是否是顺序的广告
    public static boolean checkIfFrequency(String preKey,String curKey, HashMap<String,HashMap<String,String>> next_ad){
            //String[]
        int pre_second_index = preKey.indexOf(",", preKey.indexOf(",") + 1);
        //截取的版本+批次号
        String pre_verUp=preKey.substring(0,pre_second_index);
       // String pre_pls_play=preKey.substring(pre_second_index+1,preKey.length());

        int cur_second_index = curKey.indexOf(",", curKey.indexOf(",") + 1);
        //截取的版本+批次号
        String cur_verUp=curKey.substring(0,cur_second_index);
       // String cur_pls_play=curKey.substring(cur_second_index+1,curKey.length());

        if(pre_verUp.equals(cur_verUp)){

            //先通过版本+批次号 获取Map
            //然后再根据Map中保存的seqkey 的顺序进行比较
            if(next_ad.get(pre_verUp).get(preKey).equals(curKey)){
                return true;
            }else {
                return false;
            }
        }else{
            return false;
        }
    }

    //对播放机记录进行遍历，通过已存在的seed记录来进行上下时间的修改
    public static boolean changePlayLogTimeBySeed(List<RealPlayLog> playlog_list,HashMap<String,HashMap<String,String>> next_ad,Map<String,Long> schversion_lengths)
    {

        //只要有一条记录的时间被更新了就代表还可以通过seed来更新时间
        boolean flag=false;
        RealPlayLog pre_play_log=null;
        int tmp_index=0;
        for(RealPlayLog current_playlog:playlog_list){
            tmp_index++;
            if(pre_play_log!=null){
                if(current_playlog.isseed){
                    if(!pre_play_log.isseed && checkIfFrequency(pre_play_log.seqkey,current_playlog.seqkey,next_ad)){
                        //代表顺序是对的，这里还要检查是否是连续播放的
                        long sub_play_time=current_playlog.getStart_time_long()-pre_play_log.getEnd_time_long();
                        //获取第二次出现逗号的位置用来截取 version+","+uploadcount
                        int i = current_playlog.getSeqkey().indexOf(',', current_playlog.getSeqkey().indexOf(",") + 1);
                        //判断是否是属于同一个循环，两个挨着的广告，计算他们的结束时间和开始时间的差值
                        //两条挨着的广告，相差时间小于整个版本和批次的长度，则属于一个轮播的
                        if(sub_play_time>0 && schversion_lengths.get(current_playlog.getSeqkey().substring(0,i))>sub_play_time){
                            //代表是应该是连续播放的
                            //则直接更新上一条播放记录的结束时间
                            pre_play_log.changePreEndTime(current_playlog.getStart_time_long());
                            flag=true;
                        }else if(sub_play_time==0){
                            //直接更新上一条的播放记录的开始时间
                            pre_play_log.changePreStartTime();
                            flag=true;
                        }
                    }
                }else if(pre_play_log.isseed && checkIfFrequency(pre_play_log.seqkey,current_playlog.seqkey,next_ad)){
                    long sub_time_long=current_playlog.getStart_time_long()-pre_play_log.getEnd_time_long();
                    //判断是否是属于同一个循环，两个挨着的广告，计算他们的结束时间和开始时间的差值
                    int i=current_playlog.getSeqkey().indexOf(',', current_playlog.getSeqkey().indexOf(",") + 1);
                    if(sub_time_long>0 && schversion_lengths.get(current_playlog.getSeqkey().substring(0,i))>sub_time_long){
                        //更新当前条的开始时间
                        current_playlog.replaceStartTime(pre_play_log.getEnd_time_long());
                        //logger.info("当前条不是seed，上一条是seed，更新了当前条的StartTime"+playLog.toString()+"||||||"+pre_play_log.toString());
                        flag=true;
                    }else if(sub_time_long==0){
                        //更新当前条的结束时间
                        current_playlog.replaceEndTime();
                        flag=true;
                    }
                }else if(tmp_index<playlog_list.size() && playlog_list.get(tmp_index).isseed){
                    //当前条的下一条是seed
                    if(checkIfFrequency(current_playlog.seqkey,playlog_list.get(tmp_index).seqkey,next_ad)){
                        //代表是顺序的
                        //再检查是否是再一个轮播里面的，相差时间数小于一个批次中的的总时间
                        long sub_time=playlog_list.get(tmp_index).start_time_long-current_playlog.end_time_long;

                        //获取出现第二个逗号的索引位置
                        int i=current_playlog.getSeqkey().indexOf(',', current_playlog.getSeqkey().indexOf(",") + 1);
                        if(sub_time>0 && sub_time < schversion_lengths.get(current_playlog.getSeqkey().substring(0,i))){

                            //直接更新当前条的结束时间，因为下一条是seed
                            current_playlog.completeStartAndEnd(playlog_list.get(tmp_index).getStart_time_long());
                            //logger.info("下一条才是seed，更新了上一条的EndTime和start_time"+playLog.toString()+"||||||"+pre_play_log.toString());
                            flag=true;
                        }
                    }
                }
            }
            pre_play_log=current_playlog;
        }
        return flag;
    }

    //用于生成播放记录RealPLayLog
    public  static void generateRealPlayLog(List<RealPlayLog> playlog_list,List<RealDevScanLogBean> scan_log_buf,HashMap<String,Long>  ad_length_map)
    {
        RealDevScanLogBean pre_scan_log=null;
        RealPlayLog pre_play_log=null;

        RealDevScanLogBean test=null;
        try{
            for(RealDevScanLogBean scan_log:scan_log_buf){
                test=scan_log;
                if(ad_length_map.get(scan_log.seqkey)==null){
                    continue;
                }
                if(pre_scan_log==null){
                    //代表是该设备的第一条扫描记录，直接新增一条播放记录
                    pre_scan_log=scan_log;
                    RealPlayLog playLog=new RealPlayLog(scan_log.devid,scan_log.seqkey,scan_log.getScan_time_long(),
                            1,ad_length_map.get(scan_log.seqkey),scan_log.plninfo);
                    playlog_list.add(playLog);
                    pre_play_log=playLog;
                }else{
                    //判断与上一条是否是同一条广告
                    if(scan_log.seqkey.equals(pre_scan_log.seqkey)){
                        //代表是同一条广告
                        //计算两条之间的时间差
                        long subtime=scan_log.getScan_time_long()-pre_scan_log.getScan_time_long();
                        if(subtime>=0 && ad_length_map.get(scan_log.seqkey)>=subtime){
                            //代表是同一次曝光，需要更新上次的end_time
                            long tmp_sub_time=scan_log.getScan_time_long()-pre_play_log.getEnd_time_long();
                            pre_play_log.updateEndTime(tmp_sub_time);
                        }else{
                            //代表的是不同的曝光，需要新增播放记录
                            RealPlayLog playLog=new RealPlayLog(scan_log.devid,scan_log.seqkey,scan_log.getScan_time_long(),
                                    1,ad_length_map.get(scan_log.seqkey),scan_log.plninfo);
                            playlog_list.add(playLog);
                            pre_play_log=playLog;
                        }
                    }else{
                        try{
                            //不同广告直接添加新的播放记录
                            RealPlayLog playLog=new RealPlayLog(scan_log.devid,scan_log.seqkey,scan_log.getScan_time_long(),
                                    1,ad_length_map.get(scan_log.seqkey),scan_log.plninfo);
                            playlog_list.add(playLog);
                            pre_play_log=playLog;
                        }catch (NullPointerException e){
                            System.out.println("引发空指针的数据"+scan_log.toString());
                        }

                    }
                    pre_scan_log=scan_log;
                }
            }
        }catch (NullPointerException e){
            System.out.println("最外层引发空指针异常"+test.toString());
        }

    }

    //生成设备播放记录小时的索引
    public static Map<String,Integer> gendevPlayLogMap(List<RealPlayLog> devplay_log){
        Map<String,Integer> time_playlog_index=new HashMap<String,Integer>();
        int i = 0;
        for(RealPlayLog playLog:devplay_log){
            //以小时粒度保存对应每个小时的扫描记录的第一条索引
            // yyyy-MM-dd HH:mm:ss
            String thesecond= playLog.start_time.substring(0,13);
            if(time_playlog_index.get(thesecond)==null){
                time_playlog_index.put(thesecond,i);
            }
            i += 1;
        }
        return time_playlog_index;
    }

    //获取满足当前scan_time 小时级别的第一条设备播放记的索引
    public  static int getFirstMatchSecondPlayLog(Map<String,Integer> time_playlog_index,String scan_time){
        String scan_time_hour=scan_time.substring(0,13);
        //logger.info("查找小时时间为："+scan_time+"  截取后的时间为："+scan_time_hour);
        //logger.info(time_playlog_index.get(scan_time_hour)==null?"++++++++=======Oliver是空的":time_playlog_index.get(scan_time_hour));
        return time_playlog_index.get(scan_time_hour)==null?0:time_playlog_index.get(scan_time_hour);
    }

    //用来mac插入新的mac漏掉的扫描记录
    public static List<MacScanBean> mergeAndInsertScanLog( List<MacScanBean> src_scan,List<RealPlayLog> play_log_buf,Map<String, Integer> play_second_idnex,long must_appendtime,long expose_flag_time){

        List<MacScanBean> new_scan_log=new ArrayList<MacScanBean>();
        if(play_log_buf==null || play_log_buf.size()==0){
            return src_scan;
        }
        int tmp_index=0;
        long curtime=0;//代表最开始曝光的时间
        long curEndTime=0;//代表这次曝光的截至时间
        boolean expose_end=false;  //代表这次是否已经曝光截止了
        boolean is_in_expose=false; // 代表这次增补是否是再规定的曝光时间之内
        //logger.info("DevScanLog 的第一条是："+src_scan.get(0).toString());
        int dev_playlog_index=getFirstMatchSecondPlayLog(play_second_idnex,src_scan.get(0).scan_time);
        //logger.info("获取到的的索引是："+dev_playlog_index+" 传进来的设备播放记录条数是:"+play_log_buf.size());
        //获取到mac扫描记录中在第一条该小时中，设备播放的第一条广告播放记录
        RealPlayLog dev_play_log=play_log_buf.get(dev_playlog_index);

        for(MacScanBean scanLog :src_scan){

            //dev_playlog_index=getFirstMatchSecondPlayLog(play_second_idnex,scanLog.scan_time);
            if(is_in_expose){
                is_in_expose=false;
            }
            if(expose_end){
                expose_end=false;
            }
            tmp_index+=1;
            //logger.info("当前条 scanlog  add to new_scan_log"+scanLog.toString());
            new_scan_log.add(scanLog);
            curtime=scanLog.scan_time_long;
            if(tmp_index>=src_scan.size()){
                //代表当前记录是最后一条记录
                curEndTime=curtime+must_appendtime;  //代表该mac 可能曝光的截至时间
                expose_end=true;

            }else{
                //获取同一个mac下一条的曝光记录
                MacScanBean next_mac_scan=src_scan.get(tmp_index);

                if(next_mac_scan.scan_time_long-scanLog.scan_time_long > expose_flag_time){
                    //代表超过了额5分钟了，这两条记录属于两次曝光，不是同一次
                    //则往下补指定的时间
                    curEndTime=curtime+must_appendtime;
                    //因为要补的时间差有可能超过了两条之间的时间差，所以取小的那个
                    curEndTime=next_mac_scan.scan_time_long<curEndTime?next_mac_scan.scan_time_long:curEndTime;
                    expose_end=true;
                }else{
                    //则直接将下一条的扫描记录作为结束时间
                    curEndTime=next_mac_scan.scan_time_long;
                    is_in_expose=true;
                }
            }
            //找到满足包含curtime的第一条记录
            if(dev_playlog_index<play_log_buf.size()){
                //一直找到设备播放记录时间比curtime大的第一条
                //0- 3321
                //dev_playlog_index=
                while(dev_playlog_index<play_log_buf.size() &&
                        dev_play_log.start_time_long<curtime){
                    dev_playlog_index+=1;
                    if(dev_playlog_index<play_log_buf.size()){
                        dev_play_log=play_log_buf.get(dev_playlog_index);
                    }
                }
                //logger.info("设备播放记录中开始时间大于当前mac扫描记录的第一条是："+dev_play_log.toString());
                //当上面循环退出来时就代表dev_playlog_index索引值指向的设备播放记录行就是下一条需要播的

                //开始往mac 扫描记录里面填插入新的mac扫描记录
                while(dev_playlog_index<play_log_buf.size()&& dev_play_log.start_time_long<curEndTime){
                    //logger.info("dev_play_log 的开始时间"+dev_play_log.start_time_long+"小于 curEndTime"+curEndTime);
                    String add_pln_id = dev_play_log.getPln_info();
                    if (add_pln_id==null || add_pln_id.equals("NULL")) {
                        add_pln_id = scanLog.plninfo;
                    }

                    if(expose_end || (!expose_end && dev_play_log.end_time_long<=curEndTime)){
                        //开始补这条mac的对应的被扫描的记录
                        // logger.info("dev_play_log 的结束时间"+dev_play_log.end_time_long+"小于等于 curEndTime"+curEndTime);
                        //devid,iccid,inner_sn,mac,sch_version,uploadcount,pls_index,play_index,plninfo, scan_time,sscan_time_long
                        MacScanBean insert_scanlog=null;
                        if(is_in_expose){
                            insert_scanlog=new MacScanBean(scanLog.devid,scanLog.iccid,scanLog.inner_sn,scanLog.mac,
                                    dev_play_log.sch_version+"",dev_play_log.uploadcount+"",dev_play_log.pls_index+"",dev_play_log.play_index+"",add_pln_id,
                                    dev_play_log.start_time, dev_play_log.start_time_long,0
                            );
                        }else{
                            insert_scanlog=new MacScanBean(scanLog.devid,scanLog.iccid,scanLog.inner_sn,scanLog.mac,
                                    dev_play_log.sch_version+"",dev_play_log.uploadcount+"",dev_play_log.pls_index+"",dev_play_log.play_index+"",add_pln_id,
                                    dev_play_log.start_time, dev_play_log.start_time_long,dev_play_log.start_time_long-curtime
                            );
                        }
                        new_scan_log.add(insert_scanlog);
                    }
                    dev_playlog_index+=1;
                    if(dev_playlog_index<play_log_buf.size()){
                        dev_play_log=play_log_buf.get(dev_playlog_index);
                    }
                }
            }
        }
        return new_scan_log;
    }

}
