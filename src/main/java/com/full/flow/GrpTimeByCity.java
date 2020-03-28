package com.full.flow;

import com.focusmedia.bean.MacRowCity;
import com.focusmedia.util.JavaSparkUtil;
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
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;

/*
* 按照城市粒度
* 基于two minute的方式来生成对应 1+，2+ 3+等
*
* */
public class GrpTimeByCity {
    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void generateGrpByCity(SparkSession session,JavaSparkContext context,String tdate,long seconds){

        //先获取去伪的mac_view_info数据
        Dataset<Row> fake_mac_view_info = session.sql(" select t.* from  " +
                "   (select *,substr(mac,0,6) as submac from mac_view_info where time='" + tdate + "' and city_name is not null and scan_time is not null ) t" +
                "    left join " +
                "    db01.mac_factory as p" +
                "    on t.submac=p.mac_index_factory" +
                "     where factory_name is not null ");
        fake_mac_view_info.createOrReplaceTempView("tmp_fake_macview");
        session.sql("select * from tmp_fake_macview").show();

        //获取到达两次或者两次以上的devid,mac,time记录
        Dataset<Row> base_data = session.sql("select city_name,devid,mac,scan_time,pre_scan_time, scan_new,pre_new from " +
                " (" +
                "  select city_name,devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "   (" +
                "     select city_name,devid,mac,scan_time,lag(scan_time,1) over ( partition by city_name,devid,mac order by scan_time) as pre_scan_time" +
                "       from tmp_fake_macview "+
                "    )  t " +
                " ) p where scan_new-pre_new>" + seconds + " ");
        base_data.createOrReplaceTempView("tmp_city_basedata");
        session.sql("select * from tmp_city_basedata").show();

        JavaPairRDD<String, Row> devid_row = base_data.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                // city_name,devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                return new Tuple2<String, Row>(row.get(1).toString(), row);
            }
        });

        //生成到达次数的mac记录
        JavaRDD<MacRowCity> macRowJavaRDD = devid_row.groupByKey().flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, MacRowCity>() {
            @Override
            public Iterator<MacRowCity> call(Tuple2<String, Iterable<Row>> devid_row) throws Exception {

                List<MacRowCity> result = new ArrayList<MacRowCity>();

                final Map<String, List<Row>> mac_lines = new HashMap<String, List<Row>>();

                String devid = devid_row._1;

                //因为怕这里数据量大，就先按照每个mac来进行划分来进行比较
                devid_row._2.forEach(new Consumer<Row>() {
                    @Override
                    public void accept(Row row) {
                        // city_name,devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                        if (mac_lines.get(row.get(2).toString()) == null || mac_lines.get(row.get(2).toString()).size() == 0) {
                            List<Row> mac_list = new ArrayList<Row>();
                            mac_list.add(row);
                            mac_lines.put(row.get(2).toString(), mac_list);
                        } else {
                            mac_lines.get(row.get(2).toString()).add(row);
                        }
                    }
                });

                //正对每个mac下的到达次数的记录进行处理
                for (Map.Entry<String, List<Row>> mac_entry : mac_lines.entrySet()) {
                    List<MacRowCity> mac_rows = new ArrayList<MacRowCity>();
                    String mac = mac_entry.getKey();

                    List<Row> rows = mac_entry.getValue();
                    rows.sort(new Comparator<Row>() {
                        @Override
                        public int compare(Row o1, Row o2) {
                            // city_name,devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                            long one=Long.parseLong(o1.get(5).toString());
                            long two=Long.parseLong(o2.get(5).toString());
                            return Long.compare(one,two);
                        }
                    });

                    int tmp_index=0;
                    Row pre=null;
                    for (Row line : rows) {
                        // city_name,devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                        if(tmp_index==0){
                            //代表第一条
                            MacRowCity scan_row=new MacRowCity(line.get(0).toString(),devid,line.get(2).toString(),line.get(3).toString());
                            MacRowCity pre_row=new MacRowCity(line.get(0).toString(),devid,line.get(2).toString(),line.get(4).toString());
                            mac_rows.add(scan_row);
                            mac_rows.add(pre_row);

                        }else{

                            if(pre!=null){
                                //获取上一条的scan_new,和当前条的pre_new进行比较
                                long pre_scan_new=Long.parseLong(pre.get(5).toString());
                                long cur_pre_new=Long.parseLong(line.get(6).toString());
                                if(cur_pre_new-pre_scan_new>seconds){
                                    //代表没有挨着直接将两条添加进去
                                    MacRowCity scan_row=new MacRowCity(line.get(0).toString(),devid,line.get(2).toString(),line.get(3).toString());
                                    MacRowCity pre_row=new MacRowCity(line.get(0).toString(),devid,line.get(2).toString(),line.get(4).toString());
                                    mac_rows.add(scan_row);
                                    mac_rows.add(pre_row);

                                }else{
                                    // city_name,devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                                    MacRowCity scan_row=new MacRowCity(line.get(0).toString(),devid,line.get(2).toString(),line.get(3).toString());
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

        Dataset<Row> city_expose = session.createDataFrame(macRowJavaRDD, MacRowCity.class);
        city_expose.createOrReplaceTempView("tmp_city_expose");
        session.sql("select * from tmp_city_expose").show();

        //计算出每个城市的，曝光次数大于等于 2 的对应的mac人次数
        Dataset<Row> city_twomore = session.sql("select city_name,expose_time,count(mac) as mac_nums from " +
                " (" +
                " select city_name,devid,mac,count(*) expose_time from tmp_city_expose group by city_name,devid,mac" +
                " ) m group by city_name,expose_time");
        List<Row> rows = city_twomore.collectAsList();
        System.out.println("大于等于2次的已经收集成功.................");
        //生成每个城市下的对应的大于等于2次曝光的mac 人次数
        Map<String,List<Row>> city_exp_map=new HashMap<String,List<Row>>();
        for(Row cityrow:rows){
            if(city_exp_map.get(cityrow.get(0).toString())==null){
                List<Row> sub_rows=new ArrayList<Row>();
                sub_rows.add(cityrow);
                city_exp_map.put(cityrow.get(0).toString(),sub_rows);
            }else{
                city_exp_map.get(cityrow.get(0).toString()).add(cityrow);
            }
        }


        //计算一次到达的那些mac记录

        Dataset<Row> city_one_reach = session.sql("select city_name,devid,mac, max(scan_time) as scan_time from " +
                " (" +
                " select city_name,devid,mac,scan_time,case when scan_new-pre_new<" + seconds + " then 0 else 1 end as flag  from " +
                " (" +
                " select city_name,devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "  (" +
                "    select city_name,devid,mac,scan_time,lag(scan_time,1) over ( partition by city_name,devid,mac order by scan_time) as pre_scan_time" +
                "     from tmp_fake_macview "+
                "  ) t " +
                " )" +
                " ) group by city_name,devid,mac having sum(flag)=0");
        city_one_reach.createOrReplaceTempView("tmp_one_reach");
        session.sql("select * from tmp_one_reach").show();  //这个已经打印了


        Dataset<Row> one_row = session.sql("select city_name,count(mac) as mac_nums from tmp_one_reach group by city_name");
        List<Row> city_one_map = one_row.collectAsList();
        System.out.println("一次到达的mac已经收集完成................");
        Map<String,Long> cityOnereach=new HashMap<String,Long>();
        for(Row line:city_one_map){
            if(cityOnereach.get(line.get(0).toString())==null){
                Long  times=Long.parseLong(line.get(1).toString());
                cityOnereach.put(line.get(0).toString(),times);
            }
        }
        List<CityExposeTime> result=new ArrayList<CityExposeTime>();
        //计算每个城市的1+,2+,3+,4+..........
        for(Map.Entry<String,Long> entry:cityOnereach.entrySet()){
            String city_name = entry.getKey();
            Long one_expose_city=entry.getValue();             //每一个城市的一次到达的人数
            List<Row> greatTwo = city_exp_map.get(city_name);   //每一个城市大于等于2次的到达人数
            if(greatTwo!=null && greatTwo.size()>0){
                long sum_time=0;
                for(int i=1;i<=50;i++){
                    if(i==1){
                        sum_time=one_expose_city;
                    }
                    for(Row inner_row:greatTwo){
                        int expose_times=Integer.parseInt(inner_row.get(1).toString());
                        if(expose_times>=i){
                            sum_time+=Integer.parseInt(inner_row.get(2).toString());
                        }
                    }
                    CityExposeTime cityExpose=new CityExposeTime(city_name,i,sum_time,tdate);
                    result.add(cityExpose);
                    sum_time=0;
                }
            }else{
                CityExposeTime cityExpose=new CityExposeTime(city_name,1,one_expose_city,tdate);
                result.add(cityExpose);
            }
        }
        System.out.println("已经生成最终的结果数据..................");

        JavaRDD<CityExposeTime> parallelize_rdd = context.parallelize(result);
        Dataset<Row> expose_bean_result = session.createDataFrame(parallelize_rdd, CityExposeTime.class);
        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        expose_bean_result.write().mode(SaveMode.Append).jdbc(URL,"city_ad_grp_times",pro);

    }

    public static void main(String[] args){
        String tdate=args[0];
        SparkConf conf=new SparkConf();
        conf.setAppName("grp_time_city");
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","500");
        conf.set("spark.sql.crossJoin.enabled","true");
        conf.set("spark.sql.shuffle.partitions","500");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
        // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
        conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
        conf.set("spark.locality.wait","20s");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        session.sql("use parquet_table");

        generateGrpByCity(session,context,tdate,150);
        session.stop();
    }
}
