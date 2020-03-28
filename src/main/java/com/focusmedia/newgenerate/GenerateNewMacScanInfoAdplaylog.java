package com.focusmedia.newgenerate;

import com.focusmedia.util.DevDateUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

/*
* 增补算法：
* 根据db01中的temp_sample_persons_bsgs 表中的mac选取特定的设备带增补数据
* 根据focusmedia_realtime 中的play_log_plus_txt 来获取设备的排期数据
* */

//根据现在新的realtimeschedulescreenfam来生成播放表和macview表
//一个scch_evrsion中存在多个批次，如果在mac_scan_log中的批次中没有找到对应批次下的大小顺序号
//则就去最大批次号上面去找
// 基于ad_play_log来进行计算

public class GenerateNewMacScanInfoAdplaylog {

    //代表最大曝光的分割时间,毫秒数
    public static long MAX_SUB_TIME=10*60*60*1000L;
    public static long MAX_EXPOSE_TIME=5*60*1000L;
    //public static long END_PLAY_TIME=0;
    public static void main(String[] args){

        //String tmp_devplay_table="tmp_devplay_info";
        String tmp_macview_table="";
        String appname=args[0];
        String tdate=args[1];
        String run_type="";
        // 明博的抽样表
        String sample_tablename="temp_sample_persons_week";
        String infosys_tablename="";
        String middletable = "dw_instar_middle_table";
        long must_append_time=0;    //代表往后增补多长时间
        long expose_flag_time=0;    //代表多长时间间隔才代表是两次曝光
        if(args.length>2){
            must_append_time=Integer.parseInt(args[2])*60*1000L;
        }

        if(args.length>3){
            expose_flag_time=Integer.parseInt(args[3])*60*1000L;
        }

        if(args.length>4){
            infosys_tablename=args[4];
           // tmp_devplay_table=tmp_devplay_table+"_"+table_suffix;
            tmp_macview_table=infosys_tablename;
        }

        if(args.length>5){
            run_type=args[5];
        }

        if(args.length>6){
            sample_tablename=args[6];
        }

        if(args.length>7){
            middletable=args[7];
        }
        SparkConf conf=new SparkConf();
        conf.setAppName(appname);
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","800");
        conf.set("spark.sql.crossJoin.enabled","true");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
        // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
       // conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
        conf.set("spark.locality.wait","0s");
        conf.set("spark.network.timeout","800");
        conf.set("spark.sql.parquet.compression.codec", "gzip");
        conf.set("spark.sql.shuffle.partitions","800");
        conf.set("spark.sql.hive.convertMetastoreParquet","false");
        //conf.set("spark.speculation","true");
        //conf.set("spark.speculation.interval","6000ms");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        context.setLogLevel("ERROR");
        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        spark.sql("use parquet_table");



        //显示获取需要进行增补的那些mac数据
        Dataset<Row> macsql = spark.sql(" select mac,id,city from dw_table."+sample_tablename+" where time = '" + tdate + "' group by mac,id,city ");
        long count = macsql.count();
        macsql.createOrReplaceTempView("tmp_filter_mac");
        macsql.persist(StorageLevel.MEMORY_ONLY());
        spark.sql("select * from tmp_filter_mac").show();

        if(count==0){
            System.out.println("temp_sample_persons_bsgs 分区"+tdate+" 数据为空");
            return;
        }

        List<String> partitionDate = getPartitionDate(tdate);

        // 选取每周一的fm_location_new来关联套装
        Dataset<Row> fmlocation_new = spark.sql("select devid,suit_kind,city_name,building_no,building_name from dw_table.dw_fm_location_info where time='" + partitionDate.get(partitionDate.size() - 1) + "' and install_status='已安装' and devid !='' and devicekind='液晶' and city_name in ('北京', '上海', '广州', '成都', '南京', '武汉', '深圳', '杭州', '重庆', '沈阳', '天津', '大连', '长沙', '青岛', '西安', '济南', '哈尔滨', '长春','厦门', '昆明') and devid is not null and suit_kind is not null and suit_kind !='' group by devid,suit_kind,city_name,building_no,building_name");
        fmlocation_new.persist(StorageLevel.MEMORY_ONLY());
        fmlocation_new.createOrReplaceTempView("tmp_fmlocation_new");
        spark.sql("select * from tmp_fmlocation_new").show();



        Dataset<Row> schedule_adlength = spark.sql("select adcontent,adcustomer,adproduct,max(cast(adlength as int)) as ad_length from ods_table.ods_realtime_schedule_screen_fam_by_day where time<='" + partitionDate.get(0) + "' and time>='" + partitionDate.get(partitionDate.size() - 1) + "' group by adcontent,adcustomer,adproduct");
        schedule_adlength.persist(StorageLevel.MEMORY_AND_DISK());
        schedule_adlength.createOrReplaceTempView("tmp_schedule");
        spark.sql("select * from tmp_schedule").show();


        List<Row> citys = spark.sql("select city from tmp_filter_mac  group by city").collectAsList();
        if(run_type.equals("day")){
            long tdate_long = DevDateUtil.dateStrToLong(tdate);
            String nextdate =  DevDateUtil.longDateToStr(tdate_long + 24*60*60*1000);
            System.out.println("开始取出当前日期:"+tdate+", 以及下一日期的前两个小时:"+nextdate);
            one_getMacScaninfo(spark,tdate,nextdate,must_append_time,expose_flag_time,
                    tmp_macview_table,middletable);

        }else if(run_type.equals("flow")){
            for(String mydate:partitionDate){

                //用于生成最后补齐的截至第二天凌晨时间
                long tdate_long = DevDateUtil.dateStrToLong(mydate);
                String nextdate =  DevDateUtil.longDateToStr(tdate_long + 24*60*60*1000);
                //String nexttwodate =  DevDateUtil.longDateToStr(tdate_long + 24*60*60*1000);
                System.out.println("开始取出当前日期:"+mydate+", 以及下一日期的前两个小时:"+nextdate);

                one_getMacScaninfo(spark,mydate,nextdate,must_append_time,expose_flag_time,
                        tmp_macview_table,middletable);

            }
        }

        spark.stop();
    }

    //检查为空的
    public static String checkNULL(Object obj){
        if(obj ==null){
            return "";
        }else{
            return obj.toString();
        }
    }

    //第一版 生成mac 扫描记录, 并插入漏掉的，之后再次进行合并操作
    public static void one_getMacScaninfo(SparkSession session,String tdate,String next_tdate,long must_append_time,
                                          long expose_flag_time,String tmp_macview_table,String middletable
                                          ){
        String noline_date = tdate.replace("_","");
        String noline_nextdate = next_tdate.replace("_","");
        Dataset<Row> macdata =null;
        //获取需要进行增补的所有mac数据 parquet_table.mac_scan_log_cleaned2 ?????????????????????????????测试表数据
//        macdata= session.sql(" select t.devid,t.iccid,t.inner_sn,t.mac,t.plninfo ,t.sch_version,t.uploadcount,t.pls_index,t.play_index,t.scan_time,t.signal,t.id from (" +
//                " select s.devid,s.iccid,s.inner_sn,s.mac,s.plninfo ,substr(s.sch_version,1,length(s.sch_version)-2) as sch_version,substr(s.sch_version,length(s.sch_version)-1,2) as uploadcount,s.pls_index,s.play_index,s.scan_time,s.signal,f.id" +
//                " from (select logid,devid,iccid,inner_sn,mac,plninfo,sch_version,pls_index,play_index,scan_time,log_time,online_flag,is_ap,signal,building_no,factory_name from dw_table.dw_maclog_mergeclean where time='" + tdate + "' and sch_version !='0'  and sch_version !='' group by logid,devid,iccid,inner_sn,mac,plninfo,sch_version,pls_index,play_index,scan_time,log_time,online_flag,is_ap,signal,building_no,factory_name) s inner join  (select * from tmp_filter_mac ) f on lower(s.mac)  = lower(f.mac) and substr(s.devid,1,4) = f.city" +
//                " ) t inner join (select devid from dw_table.dw_playlog_day where time like '%"+tdate+"%' group by devid ) k on t.devid = k.devid");
        macdata= session.sql(" select t.devid,t.iccid,t.inner_sn,t.mac,t.plninfo ,t.sch_version,t.uploadcount,t.pls_index,t.play_index,t.scan_time,t.signal,t.id from " +
                "    (" +
                "      select s.devid,s.iccid,s.inner_sn,s.mac,s.plninfo ,substr(s.sch_version,1,length(s.sch_version)-2) as sch_version,substr(s.sch_version,length(s.sch_version)-1,2) as uploadcount,s.pls_index,s.play_index,s.scan_time,s.signal,f.id" +
                "      from " +
                "      (   " +
                "         select r.* from (" +
                "            select k.* from (" +
                "                 select device_cyber_code as devid,iccid,device_sn as inner_sn,device_address as mac,order_id as plninfo,sch_version,pls_index,play_index," +
                "                 from_unixtime(cast(substr(cast(scan_time_stamp as string),1,10) as int),'yyyy-MM-dd HH:mm:ss') as scan_time," +
                "                 is_ap,signal_strength as signal,building_no,factory_name from dw_table.dw_mac_postprocess where time='" + tdate.replace("_","-") + "' and is_ap='0' and sch_version !='0'  and sch_version !=''" +
                "                 group by device_cyber_code,iccid,device_sn,device_address,order_id,sch_version,pls_index,play_index,scan_time_stamp,is_ap,signal_strength,building_no,factory_name" +
                "                 ) k inner join tmp_fmlocation_new m on k.devid = m.devid" +
                "            ) r inner join (select devid,mac from dw_table."+middletable+" where time='"+tdate+"' group by devid,mac) p on r.devid=p.devid and lower(r.mac) = lower(p.mac)" +
                "       ) s " +
                "       inner join  " +
                "       (select * from tmp_filter_mac ) f on lower(s.mac)  = lower(f.mac) and substr(s.devid,1,4) = f.city" +
                "   ) t inner join (select devid from dw_table.dw_playlog_day where time like '%"+tdate+"%' group by devid ) k on t.devid = k.devid");

        macdata.createOrReplaceTempView("tmp_macdata");
        macdata.persist(StorageLevel.MEMORY_AND_DISK());
        session.sql("select * from tmp_macdata").show();
        //t.devid,t.iccid,t.inner_sn,t.mac,t.plninfo ,t.buildinglist,t.uploadcount,t.pls_index,t.play_index,t.scan_time,t.signal,t.id
        JavaPairRDD<String, String> macStringRDD = macdata.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.get(0).toString(), row.get(0).toString() + "\t" + row.get(1).toString() + "\t" + row.get(2).toString() + "\t" + row.get(3).toString() + "\t"
                        + row.get(4).toString() + "\t" + row.get(5).toString() + "\t" + row.get(6).toString() + "\t" + row.get(7).toString() + "\t"
                        + checkNULL(row.get(8)) + "\t" + checkNULL(row.get(9))+"\t"+checkNULL(row.get(10))+"\t"+checkNULL(row.get(11)) );
            }
        });
        //session.sql("select * from tmp_macdata").show();

        //选取那些需要进行抽取的设备
        Dataset<Row> filterdevid = session.sql("select devid from tmp_macdata group by devid");
        filterdevid.createOrReplaceTempView("tmp_filterdevid");
        //session.sql("select * from tmp_filterdevid").show();

        // 获取原始的ad_play_log数据，并截取生成buildinglist和uploadcount????????????????????????? 测试播放表
        Dataset<Row> schedule_play = session.sql(" select d.devid,d.iccid,d.inner_sn,d.pid,d.st,d.et,d.buildinglist,d.pi,d.idx,d.adcontent,d.uploadcount " +
                " from  (select devid,iccid,inner_sn,pid,st,et,substr(sv,1,length(sv)-2) as buildinglist,pi,idx,vfn as adcontent,substr(sv,length(sv)-1,2) as uploadcount " +
                " from dw_table.dw_playlog_day where (time  like '%" + tdate + "%' or time like '%"+next_tdate+"%' )  and substr(st,1,8) in ('"+noline_date+"','"+noline_nextdate+"')  and vfn !='' and vfn is not null " +
                " group by devid,iccid,inner_sn,pid,st,et,substr(sv,1,length(sv)-2),pi,idx,vfn,substr(sv,length(sv)-1,2) ) d " +
                " inner join tmp_filterdevid m on d.devid = m.devid");
        schedule_play.createOrReplaceTempView("tmp_schedule_day");

        //session.sql("select * from tmp_schedule_day").show();

        // 和fm_location_new进行关联拿到套装， 再和排期关联拿到对应的广告长度,广告品牌，广告主
        //d.devid,d.iccid,d.inner_sn,d.pid,d.st,d.et,d.buildinglist,d.pi,d.idx,d.adcontent,d.uploadcount
        Dataset<Row> newschedule_play = session.sql(" select k.devid,k.iccid,k.inner_sn,k.pid,k.st,k.et,k.buildinglist,k.pi,k.idx,k.adcontent,k.uploadcount," +
                "   k.suit_kind,k.city_name,k.building_no,k.building_name,n.ad_length,n.adcustomer,n.adproduct  from (" +
                " select g.* , w.suit_kind,w.city_name,w.building_no,w.building_name" +
                " from tmp_schedule_day g inner join tmp_fmlocation_new w on g.devid=w.devid ) k inner join tmp_schedule n on substr(k.adcontent,1,length(k.adcontent)-4) = substr(n.adcontent,1,length(n.adcontent)-4)  ");

        newschedule_play.createOrReplaceTempView("tmp_length_screen");
        //session.sql("select * from tmp_length_screen").show();

        // 和 ods_adcontent_adid 关联拿到对应的ad_content_id
        Dataset<Row> schedule_screen = session.sql("select devid,iccid,inner_sn,pid,st,et,buildinglist,pi,idx,adcontent,uploadcount,suit_kind,city_name,ad_length,adcustomer,adproduct,ad_content_id,building_no,building_name from (" +
                " select h.devid,h.iccid,h.inner_sn,h.pid,h.st,h.et,h.buildinglist,h.pi,h.idx,h.adcontent,h.uploadcount," +
                " h.suit_kind,h.city_name,h.ad_length,h.adcustomer,h.adproduct ,j.adid as ad_content_id,h.building_no,h.building_name " +
                " from tmp_length_screen h inner join dw_table.dw_adcontent_adid j on substr(h.adcontent,1,length(h.adcontent)-4) = substr(j.adcontent,1,length(j.adcontent)-4) " +
                ") f group by  devid,iccid,inner_sn,pid,st,et,buildinglist,pi,idx,adcontent,uploadcount,suit_kind,city_name,ad_length,adcustomer,adproduct,ad_content_id,building_no,building_name");
        schedule_screen.createOrReplaceTempView("tmp_schedule_screen");
        //session.sql("select * from tmp_schedule_screen").show();


        //h.devid,h.iccid,h.inner_sn,h.pid,h.st,h.et,h.buildinglist,h.pi,h.idx,h.adcontent,h.uploadcount,
        //    h.suit_kind,h.city_name,h.building_no,h.building_name,h.ad_length,h.adcustomer,h.adproduct ,ad_content_id
        JavaPairRDD<String, String> scheduleStringRDD = schedule_screen.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.get(0).toString(), row.get(0).toString() + "\t" + row.get(1).toString() + "\t" + row.get(2).toString() + "\t" + row.get(3).toString() + "\t"
                        + row.get(4).toString() + "\t" + row.get(5).toString() + "\t" + row.get(6).toString() + "\t" + row.get(7).toString() + "\t"
                        + checkNULL(row.get(8)) + "\t" + checkNULL(row.get(9)) + "\t" + checkNULL(row.get(10)) + "\t" + checkNULL(row.get(11)) + "\t"
                        + checkNULL(row.get(12)) +"\t"+ checkNULL(row.get(13))+"\t"+checkNULL(row.get(14))+"\t"+checkNULL(row.get(15))+"\t"+checkNULL(row.get(16))+
                        "\t"+checkNULL(row.get(17))+"\t"+checkNULL(row.get(18))
                );
            }
        });
        //session.sql("select * from tmp_schedule_day").show();
        JavaPairRDD<String, String> play_and_macview = macStringRDD.union(scheduleStringRDD);
        //JavaPairRDD<String, String> play_and_macview =  session.sparkContext().union(scheduleStringRDD);
        JavaRDD<MacResultBeanAD> macViewBeanJavaRDD = play_and_macview.groupByKey(2000).flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, MacResultBeanAD>() {
            @Override
            public Iterator<MacResultBeanAD> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                System.out.println(" Task获取的devid 是： "+tuple2._1);
                //JedisCluster instance = RedisUtils.getInstance();
                //获取设备id
                String devid = tuple2._1;
                //String devid_key="devid_"+devid;
                //获取当前设备下的扫描记录和播放记录
                Iterable<String> all_info = tuple2._2;

                //封装这个设备下的每个mac的扫描记录
                final Map<String, List<MacScanVoAD>> mac_scans = new HashMap<String, List<MacScanVoAD>>();

                //封装这个设备下的播放记录
                final List<ScreenPlayVo> dev_play=new ArrayList<>();
                //用来存储最终的进过合并或者插入的mac记录
                final List<MacResultBeanAD> new_merge_mac_scan = new ArrayList<MacResultBeanAD>();

                SimpleDateFormat stet_df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                all_info.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        //System.out.println("==Oliver"+s);
                        String[] cols = s.split("\t",-1);
                        if (cols.length == 12) {
                            try{
                                //System.out.println("++++Incoming 12 ");
                                //t.devid,t.iccid,t.inner_sn,t.mac,t.plninfo ,t.buildinglist,t.uploadcount,t.pls_index,t.play_index,t.scan_time,t.signal,t.id
                                String mac = cols[3];
                                String uploadcount = Integer.parseInt(cols[6])+"";
                                long scan_time_long = DevDateUtil.timeStrToLong(cols[9]);
                                MacScanVoAD macbean = new MacScanVoAD(cols[0],cols[1],cols[2],cols[3],cols[4],cols[5],uploadcount,cols[7],cols[8],cols[9],cols[10],scan_time_long
                                ,null,null,null,null,null,null,0,cols[11],null,null);
                                if(!mac_scans.containsKey(mac)){
                                    mac_scans.put(mac,new ArrayList<>());
                                }
                                mac_scans.get(mac).add(macbean);
                            }catch (Exception e){
                                e.printStackTrace();
                                System.out.println("ERROR MAC LINE:"+s);
                            }
                        } else if (cols.length==19)  {
                            //System.out.println("++++Incoming 17 ");
                            //代表是设备播放机记录
                            long stlong =0 ;
                            long etlong = 0;

                            ScreenPlayVo playVo =null;
                            try {
                                String uploadcount = Integer.parseInt(cols[10])+"";
                                stlong = stet_df.parse(cols[4]).getTime();
                                etlong = stet_df.parse(cols[5]).getTime();
                                //d.devid,d.iccid,d.inner_sn,d.pid,d.st,d.et,d.buildinglist,d.pi,d.idx,d.adcontent,d.uploadcount,
                                //   k.suit_kind,k.city_name,n.ad_length,n.adcustomer,n.adproduct,ad_content_id,building_no,building_name
                                playVo = new ScreenPlayVo(cols[0],cols[1],cols[2],cols[3],cols[4],cols[5],cols[6],cols[7],cols[8],cols[9],
                                        uploadcount,cols[11],cols[12],cols[13],stlong,etlong,cols[14],cols[15],cols[16],cols[17],cols[18]);
                            } catch (Exception e) {
                                System.out.println("ERROR PLay Line:"+s);
                                e.printStackTrace();
                            }
                            if (playVo!=null){
                                dev_play.add(playVo);
                            }
                        }
                    }
                });

                System.out.println("devid:"+devid+"已经生成对应的播放记录和扫描记录----------总共有："+dev_play.size());
                if(dev_play.size()<=100){
                    System.out.println("ERROR:该设备"+devid+"下播放记录为0");
                    return new_merge_mac_scan.iterator();
                }
                //获取inner_sn 对应最多的扫描记录,并且根据mac进行分割
                Map<String, Integer> stringIntegerMap = null;

                dev_play.sort(new Comparator<ScreenPlayVo>() {
                    @Override
                    public int compare(ScreenPlayVo o1, ScreenPlayVo o2) {
                        int stcompare = Long.compare(o1.getStlong(), o2.getStlong());
                        if(stcompare==0){
                            int etcompare = Long.compare(o1.getEtlong(), o2.getEtlong());
                            return etcompare;
                        }else{
                            return stcompare;
                        }
                    }
                });
                System.out.println("devid："+devid+" 已经排完了序");
                long min_stlong = dev_play.get(0).getStlong();
                long max_stlong = dev_play.get(dev_play.size()-1).getStlong();
                long dimension = 20;
                int  step = (int) Math.floor((max_stlong-min_stlong)/dimension);
                // 根据播放记录的开始时间的long值，将播放记录划分到各个时间区间中，默认20个区间
                TreeMap<Long,List<ScreenPlayVo>> hour_playlog = new TreeMap<>();
                while(min_stlong<=max_stlong){
                    hour_playlog.put(min_stlong,new ArrayList<>());
                    min_stlong = min_stlong + step;
                }
                System.out.println("LOOP has Success........................");
                Object[] objects = hour_playlog.keySet().toArray();
                for(ScreenPlayVo sc :dev_play){
                    for(int i=0;i<objects.length;i++){
                        if(i!=objects.length-1){
                            //当还没到最后一个key 的时候
                            if(sc.getStlong()>=(long)objects[i] && sc.getStlong() <(long)objects[i+1]){
                                hour_playlog.get((long)objects[i]).add(sc);
                                break;
                            }
                        }else{
                            // 已经到最后一个 索引了，则肯定是大于等于最后一个key了
                            hour_playlog.get((long)objects[i]).add(sc);
                        }
                    }
                }

              /*  System.out.println("=========devid："+devid+" Has Split Time");
                for(int i=0;i<objects.length;i++){
                    System.out.println(objects[i]);
                }*/

                //将每个小时中的数据按照升序进行排序
                for(Map.Entry<Long, List<ScreenPlayVo>> entry:hour_playlog.entrySet()){
                    entry.getValue().sort(new Comparator<ScreenPlayVo>() {
                        @Override
                        public int compare(ScreenPlayVo o1, ScreenPlayVo o2) {
                            int stcompare = Long.compare(o1.getStlong(), o2.getStlong());
                            if(stcompare==0){
                                int etcompare = Long.compare(o1.getEtlong(), o2.getEtlong());
                                return etcompare;
                            }else{
                                return stcompare;
                            }
                        }
                    });
                }
                //System.out.println("devid："+devid+" 每个时间段中的数据已经排好了序");

                //对当前设备的下的mac 扫描的所有扫描记录进行处理
               // System.out.println("设备："+devid+"处理开始时间"+new Date()+": 总mac数:"+mac_scans.keySet().size());
                for (Map.Entry<String, List<MacScanVoAD>> entry : mac_scans.entrySet()) {
                    String mac = entry.getKey();
                    //System.out.println("增补：对Mac"+mac+" 数据进行处理");
                    List<MacScanVoAD> mac_scan_list = entry.getValue();
                    if(mac_scan_list.size()>1000){
                        continue;
                    }
                   // System.out.println("设备："+devid+"下 Mac数据 处理开始"+mac+"|"+entry.getValue().size()+"|"+new Date());
                    mac_scan_list.sort(new Comparator<MacScanVoAD>() {
                        @Override
                        public int compare(MacScanVoAD o1, MacScanVoAD o2) {
                            return Long.compare(o1.scantimelong, o2.scantimelong);
                        }
                    });
                    //增补对应的mac扫描记录
                    //System.out.println("mac "+mac+"开始准备进入增补程序------");
                    List<MacScanVoAD> macScanBeans = mergeAndInsertScanLog(objects,mac_scan_list, hour_playlog, must_append_time,expose_flag_time);
                    //System.out.println("mac "+mac+"增补完成+++++++++");
                    //System.out.println("开始准备进入合并程序");
                    //在对插入新的mac扫描记录之后的list进行合并处理，并生成end_time,end_time_long字段
                    List<MacResultBeanAD> macViewBeans = combineMacScanBean(macScanBeans,expose_flag_time);

                    new_merge_mac_scan.addAll(macViewBeans);
                    /*if(new_merge_mac_scan.size()>2000000){
                        System.out.println("oliver==============该设备"+devid+"增补后的数据量超过了2000000");
                        //return new_merge_mac_scan.iterator();
                    }*/

                }
                //System.out.println("设备："+devid+"处理结束时间"+new Date()+"| 最后devid增补记录为:"+new_merge_mac_scan.size());
                //throw new Exception("++++++++++++++++++++test ");
                return new_merge_mac_scan.iterator();
            }
        });

        Dataset<Row> macViewDataFrame = session.createDataFrame(macViewBeanJavaRDD, MacResultBeanAD.class);
        macViewDataFrame.createOrReplaceTempView("tmp_macview");
        // session.sql("select count(*) from tmp_macview").show();

        //session.sql("select * from tmp_macview where mac='38a4edbe7839'").show(300);
        macViewDataFrame.repartition(300).write().mode(SaveMode.Overwrite).parquet("/user/lichen/adinfosys/"+tdate);
        saveMacViewInfo(session,tmp_macview_table,tdate);

        macdata.unpersist();
    }


    //用来mac插入新的mac漏掉的扫描记录
    public static List<MacScanVoAD> mergeAndInsertScanLog( Object[] objects,List<MacScanVoAD> src_scan,Map<Long,List<ScreenPlayVo>> hour_playlog,long must_appendtime,long expose_flag_time) throws Exception {

        List<MacScanVoAD> new_scan_log=new ArrayList<MacScanVoAD>();
        if(hour_playlog==null || hour_playlog.size()==0){
            return src_scan;
        }
        int tmp_index=0;
        long curtime=0;//代表最开始曝光的时间
        long curEndTime=0;//代表这次曝光的截至时间

        //logger.info("获取到的的索引是："+dev_playlog_index+" 传进来的设备播放记录条数是:"+play_log_buf.size());
        //获取到mac扫描记录中在第一条该小时中，设备播放的第一条广告播放记录

        for(MacScanVoAD scanLog :src_scan){
            boolean is_in_expose=false; // 代表这次增补是否是再规定的曝光时间之内
            boolean expose_end=false; //代表这次是否已经曝光截止了

            //比较当前记录是再那个key区间里面
            int flag_index=0;
            for(int i=0;i<objects.length;i++){
                if(i!=objects.length-1){
                    //先找到属于那个区间里面，然后再找对应的区间中的list
                    if(scanLog.getScantimelong()>=(long)objects[i] && scanLog.getScantimelong() <(long)objects[i+1]){
                        if(i==0){
                            flag_index=0;
                        }else{
                            flag_index=i-1;
                        }
                        // 既然找到了索引项，则直接退出
                        break;
                    }
                }else{
                    flag_index = i-1;
                }
            }

            for(int j=flag_index;j<objects.length;j++){
                for(ScreenPlayVo playbean:hour_playlog.get((long)objects[j])){
                    //System.out.println("ONE_COMPARE:"+scanLog.getScantimelong()+"| PLAY_COMPARE:"+playbean.getStlong()+","+playbean.getEtlong());
                    if(playbean.getStlong()<=scanLog.getScantimelong() && scanLog.getScantimelong() <=playbean.getEtlong()){
                        scanLog.setAdcontent(playbean.getAdcontent());
                        scanLog.setAd_content_id(playbean.getAd_content_id());
                        scanLog.setSuit_kind(playbean.getSuit_kind());
                        scanLog.setAd_length(playbean.getAd_length());
                        scanLog.setAdproduct(playbean.getAdproduct());
                        scanLog.setAdcustomer(playbean.getAdcustomer());
                        scanLog.setCity_name(playbean.getCityname());
                        scanLog.setBuildingno(playbean.getBuilingno());
                        scanLog.setBuildingname(playbean.getBuildingname());
                        //System.out.println("ONE_SUCCEE:"+scanLog.getScantimelong()+"| PLAY_COMPARE:"+playbean.getStlong()+","+playbean.getEtlong());
                        break;
                    }
                }
            }


            //测试-------------
            //System.out.println("SCANLOG+++++++++++++++"+scanLog.toString());

            //dev_playlog_index=getFirstMatchSecondPlayLog(play_second_idnex,scanLog.scan_time);
            tmp_index+=1;
            //logger.info("当前条 scanlog  add to new_scan_log"+scanLog.toString());
            new_scan_log.add(scanLog);
            curtime=scanLog.scantimelong;
            if(tmp_index>=src_scan.size()){
                //代表当前记录是最后一条记录
                curEndTime=curtime+must_appendtime;  //代表该mac 可能曝光的截至时间
                expose_end=true;

            }else{
                //获取同一个mac下一条的曝光记录
                MacScanVoAD next_mac_scan=src_scan.get(tmp_index);

                if(next_mac_scan.scantimelong-scanLog.scantimelong > expose_flag_time){
                    //代表超过了额5分钟了，这两条记录属于两次曝光，不是同一次
                    //则往下补指定的时间
                    curEndTime=curtime+must_appendtime;
                    //因为要补的时间差有可能超过了两条之间的时间差，所以取小的那个
                    curEndTime=next_mac_scan.scantimelong<curEndTime?next_mac_scan.scantimelong:curEndTime;
                    expose_end=true;
                }else{
                    //则直接将下一条的扫描记录作为结束时间
                    curEndTime=next_mac_scan.scantimelong;

                    //代表是小于指定5分众之内的增补
                    is_in_expose=true;
                }
            }
            if(tmp_index>=src_scan.size()  || expose_end ){
                //增补大于五分钟和最后一条记录
                //System.out.println("====到达次数大于5分钟或者最后一条记录增补");
                sulementMacVo(objects,scanLog,scanLog.scantimelong,curEndTime,hour_playlog,new_scan_log,true);
            }else if(is_in_expose){
                //增补小于五分钟之内的
                //System.out.println("====小于5分钟记录增补");
                sulementMacVo(objects,scanLog,scanLog.scantimelong,curEndTime,hour_playlog,new_scan_log,false);
            }
        }
        return new_scan_log;
    }


    //增补mac的封装方法
    public static void sulementMacVo(Object[] objects,MacScanVoAD scanLog,long scantimelong,long curEndTime,
                                       Map<Long,List<ScreenPlayVo>> hour_playlog,
                                     List<MacScanVoAD> new_scan_log,boolean exposeflag) throws Exception {
        //代表是最后一条 或者是 两个记录时间像个大于了对应的到达时间, 接着往后面补
        for(int i=0;i<objects.length;i++){
            if(i!=objects.length-1){
                if(scanLog.getScantimelong()>=(long)objects[i] && scanLog.getScantimelong() <(long)objects[i+1]){
                    //代表再当前区间
                    //System.out.println("ADD-------INFOSYSADD had Incoming："+objects[i]+"|"+objects[i+1]);
                    List<ScreenPlayVo> screenPlayVos = hour_playlog.get((long)objects[i]);
                    //System.out.println("ADD-------GET Objects Key is："+objects[i]+" and i  is :"+i);
                    insertMac(objects,i,curEndTime,exposeflag,scanLog,new_scan_log,screenPlayVos,hour_playlog);
                    break;
                }
            }else{
                //代表最后一个区间
                //System.out.println("LAST ADD ========GET Objects Key is："+objects[i]);
                List<ScreenPlayVo> screenPlayVos = hour_playlog.get((long)objects[i]);
                //System.out.println("======== 最后段获取的播放列表数量："+screenPlayVos.size());
                insertMac(objects,i,curEndTime,exposeflag,scanLog,new_scan_log,screenPlayVos,hour_playlog);
            }
        }

    }

    //插入对应的mac记录
    /*
    * @param objects : 播放记录时间分段的各个段值
    * @param current_index : 代表当前开始增补的对应的段值再objects的下标索引
    * @param curEndTime : 增补的截止时间
    * @param exposeflag : 代表是再5分钟之内 还是大于5分钟
    *
    * */
    public static void insertMac(Object[] objects,int current_index,long curEndTime,boolean exposeflag, MacScanVoAD scanLog,
                                 List<MacScanVoAD> new_scan_log,List<ScreenPlayVo> screenPlayVos,Map<Long,List<ScreenPlayVo>> hour_playlog){

        MacScanVo pre_addmaclog = null;
        boolean flag = false;
        long timediff = 0;
       // System.out.println("当前扫描记录："+scanLog.toString()+"|"+"当前增补截止时间："+curEndTime);
        for(int i =0;i<screenPlayVos.size();i++){
            ScreenPlayVo playVo = screenPlayVos.get(i);
            if((playVo.getStlong()<=scanLog.scantimelong && scanLog.scantimelong<=playVo.getEtlong())
                    || playVo.getEtlong() <scanLog.scantimelong){
                continue;
            }else{
                //System.out.println("Oliver_Insert："+playVo.toString());
                if(exposeflag){
                    timediff=playVo.stlong-scanLog.scantimelong;
                }
                String add_scantime = DevDateUtil.playdatetoScan(playVo.stlong+1000);
               // System.out.println("ADD_SCANTIME|||||||||||||||"+add_scantime+"---"+playVo.stlong);
                //当该条播放记录的开始时间已经比curendtime还要大时，就不用再进行增补了
                if(playVo.stlong>=curEndTime){
                    flag = true;
                    break;
                }
                MacScanVoAD current_addmac = new MacScanVoAD(scanLog.devid,scanLog.iccid,scanLog.inner_sn,scanLog.mac,playVo.pid
                        ,playVo.sv,playVo.uploadcount,playVo.pi,playVo.idx,add_scantime,scanLog.signal,playVo.stlong+1000,playVo.getAdcustomer(),
                        playVo.getAdproduct(),playVo.adcontent,playVo.ad_content_id,playVo.buildingname,playVo.builingno,timediff,scanLog.getPeopleid(),
                        playVo.getSuit_kind(),playVo.getAd_length());
                current_addmac.setCity_name(playVo.cityname);
                new_scan_log.add(current_addmac);
               // System.out.println("ADD_MACSCAN---------------------"+current_addmac.toString());
               // pre_addmaclog = current_addmac;
            }
        }

        if(!flag){
            //代表上一小时补完了却还没到curEndTime
            //接在再补下一小时的
            if(current_index!=objects.length-1){
                List<ScreenPlayVo> nexthour_plays = hour_playlog.get((long)objects[current_index+1]);
                //System.out.println("---======== Next_Oliver_INSERT Hour is ："+objects[current_index+1]);
                //System.out.println("---======== Next_Oliver_INSERT: First value is "+nexthour_plays.get(0).toString());
                //System.out.println("---======== Next_Oliver_INSERT: Base line is  "+curEndTime);
                try{
                    if(nexthour_plays !=null && nexthour_plays.size()>0){
                        for(int i =0;i<nexthour_plays.size();i++){
                            //因为已经是下一个小时了，所以不用比较时间是否包含之内的
                            ScreenPlayVo playVo = nexthour_plays.get(i);
                            if(exposeflag){
                                timediff=playVo.stlong-scanLog.scantimelong;
                            }
                            String add_scantime = DevDateUtil.playdatetoScan(playVo.stlong+1000);
                            if(playVo.stlong>=curEndTime){
                                break;
                            }
                            MacScanVoAD current_addmac = new MacScanVoAD(scanLog.devid,scanLog.iccid,scanLog.inner_sn,scanLog.mac,playVo.pid
                                    ,playVo.sv,playVo.uploadcount,playVo.pi,playVo.idx,add_scantime,scanLog.signal,playVo.stlong+1000,playVo.getAdcustomer(),
                                    playVo.getAdproduct(),playVo.adcontent,playVo.ad_content_id,playVo.buildingname,playVo.builingno,timediff,scanLog.getPeopleid(),
                                    playVo.getSuit_kind(),playVo.getAd_length());
                            current_addmac.setCity_name(playVo.cityname);
                            new_scan_log.add(current_addmac);
                           // System.out.println("NEXT_HOUR_INSERT:"+current_addmac.toString());
                            //pre_addmaclog = current_addmac;

                        }
                       // System.out.println("---======== Next_Oliver_INSERT: Has Been Complete ");
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    //System.out.println("当前小时为:"+currenthour +",下一小时数据量为:"+nexthour_plays.size());
                    //throw new Exception("当前小时为:"+currenthour +",下一小时数据量为:"+nexthour_plays.size());
                }
            }
        }
    }


    public static void saveMacViewInfo(SparkSession session,String macViewTable,String tdate){

        session.sql("CREATE TABLE IF NOT EXISTS dw_table."+macViewTable+" ( " +
                " ad_content_id string,ad_length string,adcontent string,adcustomer string,adproduct string," +
                " buildingname string,buildingno string,devid string,end_time string,end_time_long long," +
                " iccid string,inner_sn string,mac string,peopleid string,play_index string,plninfo string," +
                " pls_index string,sch_version string,start_time string,start_time_long long,suit_kind string,time_diff long,uploadcount string,city_name string)" +
                " PARTITIONED BY (time string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
                " ");
        /*session.sql("insert overwrite table "+macViewTable+" partition(time='" + tdate + "') select " +
                " devid,iccid,inner_sn,mac,sch_version,uploadcount,pls_index,play_index,plninfo,start_time," +
                " start_time_long,end_time,end_time_long from tmp_macview ");*/
        session.sql("load data inpath '/user/lichen/adinfosys/"+tdate+"/*.parquet' overwrite into table dw_table."+macViewTable+" partition (time='"+tdate+"')");

    }

    //对生成mac扫描记录进行合并处理
    public static List<MacResultBeanAD> combineMacScanBean(List<MacScanVoAD> macScanBeans,long expose_flag_time) throws Exception {

        //测试打印
        /*for(MacScanVo vo :macScanBeans){
            System.out.println("PRINT#############"+vo.toString());
        }*/

        List<MacResultBeanAD> mac_results=new ArrayList<MacResultBeanAD>();
        //代表前一条 mac view扫描记录
        MacResultBeanAD pre_mac_viewlog = null;
        int tmp_index=0;
        boolean flag=true;
        for (MacScanVoAD cur_exposelog : macScanBeans) {
            //System.out.println("Start Cmbine:"+cur_exposelog.toString()+" 开始合并处理");
            tmp_index++;
            if (pre_mac_viewlog != null) {
                //System.out.println("++++++++++++++PreSacnlog:"+pre_mac_viewlog.toString());
                try{
                    //拿当前条和上一条进行对比scan_time_long时间的对比
                    if (cur_exposelog.scantimelong - pre_mac_viewlog.end_time_long > expose_flag_time) {
                        //代表了是不同的曝光,则直接生成新的mac扫描记录
                        long start_time_long = DevDateUtil.timeStrToLong(cur_exposelog.scan_time);
                        long end_time_long = start_time_long + 1L;
                        String end_time = DevDateUtil.longTimeToStr(end_time_long);
                        MacResultBeanAD viewLog = new MacResultBeanAD(cur_exposelog.devid, cur_exposelog.iccid, cur_exposelog.inner_sn,
                                cur_exposelog.mac,  cur_exposelog.sch_version,cur_exposelog.uploadcount, cur_exposelog.pls_index, cur_exposelog.play_index
                                ,cur_exposelog.plninfo, cur_exposelog.scan_time, start_time_long, end_time, end_time_long,cur_exposelog.time_diff,cur_exposelog.adcustomer,
                                cur_exposelog.adproduct,cur_exposelog.adcontent,cur_exposelog.ad_content_id,cur_exposelog.buildingname,cur_exposelog.buildingno,
                                cur_exposelog.getPeopleid(),cur_exposelog.getSuit_kind(),cur_exposelog.ad_length);
                        viewLog.setCity_name(cur_exposelog.city_name);
                        mac_results.add(viewLog);
                        pre_mac_viewlog = viewLog;
                        //System.out.println("++++++GREATER FIVE CURRENT:"+cur_exposelog.toString()+"===GREATER VIEWLOG:"+viewLog.toString());
                    } else {
                        //代表是同一次曝光，需要将下一条的scan_time赋值给上一条的end_time，并且新增曝光记录

                        //接着判断两个紧挨着的是否同一个广告，如果是的直接合并
                        //如果出现了在一个循环中中途重启了，则重启后重新开始的也会创建为一个新的曝光记录
                        if (cur_exposelog.getAd_length()!=null && cur_exposelog.sch_version.equals(pre_mac_viewlog.sch_version) &&
                                cur_exposelog.uploadcount.equals(pre_mac_viewlog.uploadcount) &&
                                cur_exposelog.pls_index.equals(pre_mac_viewlog.pls_index) &&
                                cur_exposelog.play_index.equals(pre_mac_viewlog.play_index) &&
                                cur_exposelog.scantimelong-pre_mac_viewlog.start_time_long<=Long.parseLong(cur_exposelog.ad_length)*1000) {
                            //代表是同一条广告的扫描记录，并且两条时间差小于等于当前条的广告长度,这个时候只需要更新上一条的时间不需要新增mac 曝光记录
                            pre_mac_viewlog.updateEndTime(cur_exposelog.scantimelong);
                            flag=false;
                           // System.out.println("-------UPDATE PREVIEWLOG:"+cur_exposelog.toString());

                        } else {
                            //再将这一次曝光的不同广告记录新增进去
                            long start_time_long = DevDateUtil.timeStrToLong(cur_exposelog.scan_time);
                            long end_time_long = start_time_long + 1L;
                            String end_time = DevDateUtil.longTimeToStr(end_time_long);
                            MacResultBeanAD viewLog = new MacResultBeanAD(cur_exposelog.devid, cur_exposelog.iccid, cur_exposelog.inner_sn,
                                    cur_exposelog.mac,  cur_exposelog.sch_version, cur_exposelog.uploadcount,cur_exposelog.pls_index, cur_exposelog.play_index
                                    ,cur_exposelog.plninfo, cur_exposelog.scan_time, start_time_long, end_time, end_time_long,cur_exposelog.time_diff,cur_exposelog.adcustomer,
                                    cur_exposelog.adproduct,cur_exposelog.adcontent,cur_exposelog.ad_content_id,cur_exposelog.buildingname,cur_exposelog.buildingno,
                                    cur_exposelog.getPeopleid(),cur_exposelog.getSuit_kind(),cur_exposelog.ad_length);
                            viewLog.setCity_name(cur_exposelog.city_name);
                            mac_results.add(viewLog);
                            pre_mac_viewlog = viewLog;
                            //System.out.println("&&&&&&&&& DIFFERENT CURRENT:"+viewLog.toString());
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.print("ERROR_DATE:"+cur_exposelog.toString());
                    //throw  new Exception("ERROR_DATE:"+cur_exposelog.toString());
                }
            } else {

                //代表是该mac的第一条扫描记录，直接新增mac view log  记录
                long start_time_long = cur_exposelog.scantimelong;
                long end_time_long = start_time_long + 1L;
                String end_time = DevDateUtil.longTimeToStr(end_time_long);
                MacResultBeanAD viewLog = new MacResultBeanAD(cur_exposelog.devid, cur_exposelog.iccid, cur_exposelog.inner_sn,
                        cur_exposelog.mac,  cur_exposelog.sch_version, cur_exposelog.uploadcount,cur_exposelog.pls_index, cur_exposelog.play_index
                        ,cur_exposelog.plninfo, cur_exposelog.scan_time, start_time_long, end_time, end_time_long,cur_exposelog.time_diff,cur_exposelog.adcustomer,
                        cur_exposelog.adproduct,cur_exposelog.adcontent,cur_exposelog.ad_content_id,cur_exposelog.buildingname,cur_exposelog.buildingno,
                        cur_exposelog.getPeopleid(),cur_exposelog.getSuit_kind(),cur_exposelog.ad_length);
                viewLog.setCity_name(cur_exposelog.city_name);
                mac_results.add(viewLog);
                pre_mac_viewlog = viewLog;
                //System.out.println("|||||||FIRST ERROR CURRENT:"+cur_exposelog.toString()+"===FIRST VIEWLOG:"+viewLog.toString());
            }
            flag=true;
            //System.out.println(" END Mac Data:"+cur_exposelog.toString()+" 结束合并处理");
        }
        //throw new Exception("Test----------------");
        return mac_results;
    }

    //根据指定的周日的时间，来获取对应的
    public static List<String> getPartitionDate(String tdate){
        List<String> result = new ArrayList<String>();
        SimpleDateFormat df = new SimpleDateFormat("yyyy_MM_dd");
        Calendar cal =Calendar.getInstance();
        try {
            //获取周日这一天的日期
            Date satday = df.parse(tdate);
            cal.setTime(satday);
            for(int i=0;i<=6;i++){
                cal.add(Calendar.DATE,-i);
                Date predate = cal.getTime();
                result.add(df.format(predate));
                cal.setTime(satday);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

}
