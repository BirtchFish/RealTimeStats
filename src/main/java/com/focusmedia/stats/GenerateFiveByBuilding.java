package com.focusmedia.stats;

import com.focusmedia.bean.MacRow;
import com.focusmedia.bean.MacRowBuilding;
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

import java.util.*;
import java.util.function.Consumer;

/*
生成基于5分钟的按照楼宇来进行计算mac到达的
 */
public class GenerateFiveByBuilding {

    public static void main(String[] args){

        String appname=args[0];

        String tdate=args[1];
        //对应秒数
        int minute=Integer.parseInt(args[2]);

        String tablename=args[3];

        SparkSession session = JavaSparkUtil.getRemoteSparkSession(appname, "parquet_table");

        generateResultInfo(session,tdate,minute,tablename);

        session.stop();
    }


    public static void  generateResultInfo(SparkSession session,String tdate,int seconds,String tablename){

        /*  devid,mac,scan_time,pre_scantime
        *   Dev1 YWEY   10       10
            Dev1 YWEY   170      10
            Dev1 YWEY   400      170
            Dev1 YWEY   410      400
            Dev2 YWEY   10       10
            Dev2 YWEY   170      10
            Dev2 YWEY   400      170
            Dev2 YWEY   410      400

        *   找出相差大于默认2.5分钟的
        *     Dev1 YWEY   170      10
              Dev1 YWEY   400      170
              Dev2 YWEY   170      10
              Dev2 YWEY   400      170
             那么最后的记录应该是
              Dev1 YWEY 10
              Dev1 YWEY  170
              Dev1 YWEY 400
              Dev2 YWEY 10
              Dev2 YWEY  170
              Dev2 YWEY 400
        * */
        //先将mac和楼宇数进行关联后，剔除那些在这一栋楼里面，扫描到50次以上的mac
        System.out.println("开始处理的日期是："+tdate);

        //获取当天的设备，城市，楼宇对应信息
        Dataset<Row> loc = session.sql("" +
                " select t.devid,p.building_name,p.city_name from " +
                "  (select devid,max(update_time) as update_time from fm_location where time='" + tdate + "' group by devid) t" +
                " left join" +
                " (" +
                "    select devid,building_no,building_name,city_no,city_name,district_name,suit_kind,update_time," +
                "       max(install_address) install_address,max(install_location) install_location,max(install_detail_location) install_detail_location," +
                "       max(install_area) install_area" +
                "      from fm_location where time='" + tdate + "' and install_status='已安装' " +
                "     group by devid,building_no,building_name,city_no,city_name,district_no,district_name,suit_kind,update_time" +
                " ) p" +
                " on t.devid=p.devid" +
                " where t.update_time=p.update_time");
        loc.createOrReplaceTempView("tmp_fm_location");

        Dataset<Row> src_data = session.sql(" select m.*,f.building_name,f.city_name from  " +
                " ( select * from mac_scan_log where time='" + tdate + "' ) m " +
                " inner join tmp_fm_location f " +
                " on m.devid=f.devid");
        src_data.createOrReplaceTempView("tmp_src_data");
        session.sql("select * from tmp_src_data").show();
        //选出在楼里面出现扫描次数达到50次的
        Dataset<Row> exclude_data = session.sql("select city_name as cityname,building_name as buildingname,mac as ex_mac ,count(*) as num  from tmp_src_data group by city_name,building_name,mac having count(*) >50 ");
        exclude_data.createOrReplaceTempView("tmp_exclude_data");
        session.sql("select * from tmp_exclude_data").show();
        //然后剔除mac扫描数量超过50次的mac扫描记录,取出那些和大于50次的mac关联不上的mac数据
        Dataset<Row> mac_scan_data = session.sql(" select * from " +
                " ( " +
                " select a.*,b.num from tmp_src_data a left join tmp_exclude_data b " +
                " on city_name=cityname and building_name=buildingname and mac=ex_mac " +
                " ) where num is null ");
        mac_scan_data.createOrReplaceTempView("tmp_mac_scan_data");
        session.sql("select * from tmp_mac_scan_data ").show();
        //获取到达两次或者两次以上的devid,mac,time记录
        Dataset<Row> base_data = session.sql("select city_name,building_name,mac,scan_time,pre_scan_time, scan_new,pre_new from " +
                " (" +
                "  select city_name,building_name,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "   (" +
                "     select city_name,building_name,mac,scan_time,lag(scan_time,1) over ( partition by city_name,building_name,mac order by scan_time) as pre_scan_time" +
                "       from tmp_mac_scan_data " +
                "    )  t " +
                " ) p where scan_new-pre_new>=" + seconds + " ");

        JavaPairRDD<String, Row> devid_row = base_data.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                // city_name,building_name,mac,scan_time,pre_scan_time,scan_new,pre_new
                //根据城市和楼宇来进行分组
                return new Tuple2<String, Row>(row.get(0).toString()+"#"+row.get(1).toString(), row);
            }
        });

        //生成到达次数的mac记录
        JavaRDD<MacRowBuilding> macRowJavaRDD = devid_row.groupByKey().flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, MacRowBuilding>() {
            @Override
            public Iterator<MacRowBuilding> call(Tuple2<String, Iterable<Row>> devid_row) throws Exception {

                List<MacRowBuilding> result = new ArrayList<MacRowBuilding>();

                final Map<String, List<Row>> mac_lines = new HashMap<String, List<Row>>();

                String[] city_name_buildname = devid_row._1.split("#");

                //因为怕这里数据量大，就先按照每个mac来进行划分来进行比较
                devid_row._2.forEach(new Consumer<Row>() {
                    @Override
                    public void accept(Row row) {
                        // city_name,building_name,mac,scan_time,pre_scan_time,scan_new,pre_new
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
                    List<MacRowBuilding> mac_rows = new ArrayList<MacRowBuilding>();
                    String mac = mac_entry.getKey();


                    List<Row> rows = mac_entry.getValue();
                    rows.sort(new Comparator<Row>() {
                        @Override
                        public int compare(Row o1, Row o2) {
                            //devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                            //city_name,building_name,mac,scan_time,pre_scan_time,scan_new,pre_new
                            long one=Long.parseLong(o1.get(5).toString());
                            long two=Long.parseLong(o2.get(5).toString());
                            return Long.compare(one,two);
                        }
                    });

                    int tmp_index=0;
                    Row pre=null;
                    for (Row line : rows) {
                        // devid,mac,scan_time,pre_scan_time,scan_new,pre_new
                        //city_name,building_name,mac,scan_time,pre_scan_time,scan_new,pre_new
                        if(tmp_index==0){
                            //代表第一条
                            MacRowBuilding scan_row=new MacRowBuilding(city_name_buildname[0],city_name_buildname[1],line.get(2).toString(),line.get(3).toString());
                            MacRowBuilding pre_row=new MacRowBuilding(city_name_buildname[0],city_name_buildname[1],line.get(2).toString(),line.get(4).toString());
                            mac_rows.add(scan_row);
                            mac_rows.add(pre_row);

                        }else{

                            if(pre!=null){
                                //获取上一条的scan_new,和当前条的pre_new进行比较
                                //100  50
                                //170 110
                                long pre_scan_new=Long.parseLong(pre.get(5).toString());
                                long cur_pre_new=Long.parseLong(line.get(6).toString());
                                if(cur_pre_new-pre_scan_new>seconds){
                                    //代表没有挨着直接将两条添加进去
                                    MacRowBuilding scan_row=new MacRowBuilding(city_name_buildname[0],city_name_buildname[1],line.get(2).toString(),line.get(3).toString());
                                    MacRowBuilding pre_row=new MacRowBuilding(city_name_buildname[0],city_name_buildname[1],line.get(2).toString(),line.get(4).toString());
                                    mac_rows.add(scan_row);
                                    mac_rows.add(pre_row);

                                }else{
                                    MacRowBuilding scan_row=new MacRowBuilding(city_name_buildname[0],city_name_buildname[1],line.get(2).toString(),line.get(3).toString());
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


        Dataset<Row> dataFrame = session.createDataFrame(macRowJavaRDD, MacRowBuilding.class);


        //计算一次到达的那些mac记录

        Dataset<Row> one_reach = session.sql("select city_name,building_name,mac, max(scan_time) as scan_time from " +
                " (" +
                " select city_name,building_name,mac,scan_time,case when scan_new-pre_new<" + seconds + " then 0 else 1 end as flag  from " +
                " (" +
                " select city_name,building_name,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "  (" +
                "    select city_name,building_name,mac,scan_time,lag(scan_time,1) over ( partition by city_name,building_name,mac order by scan_time) as pre_scan_time" +
                "     from tmp_mac_scan_data " +
                "  ) t " +
                " )" +
                " ) group by city_name,building_name,mac having sum(flag)=0");

        Dataset<Row> all_devidmac = dataFrame.union(one_reach);

        all_devidmac.createOrReplaceTempView("tmp_expose");


        //保存表数据
        saveTwoMinuteTable(session,tablename,tdate,"parquet_table");

    }

    //保存生成的表
    public static void saveTwoMinuteTable(SparkSession session,String tablename,String tdate,String dblocation){
        session.sql("CREATE  TABLE IF NOT EXISTS "+tablename+" (" +
                "city_name string," +
                "building_name string, " +
                "mac string," +
                "scan_time string )" +
                " PARTITIONED BY (time string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " );

        Dataset<Row> result_dataset = session.sql("select city_name,building_name,mac,scan_time from tmp_expose");
        result_dataset.repartition(50).write().mode(SaveMode.Overwrite).parquet("/parquet_fiveminute/"+tdate);
        session.sql(" load data inpath '/parquet_fiveminute/"+tdate+"/*.parquet' overwrite into table "+tablename+" partition (time='"+tdate+"')");
       /* session.sql("insert overwrite table "+tablename+" partition(time='" + tdate.replace("_","-") + "') select " +
                "devid,mac,scan_time from tmp_expose");*/
    }

}
