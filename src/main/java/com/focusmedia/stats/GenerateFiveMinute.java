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
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

/*
生成基于2.5分钟的原生的devid,mac,scan_time记录
 */
public class GenerateFiveMinute {

    public static void main(String[] args){

        String tdate=args[0];
        //对应秒数
        int minute=Integer.parseInt(args[1]);

        int num_filter = Integer.parseInt(args[2]);
        int hour_filter = Integer.parseInt(args[3]);
        String run_type = args[4];

        String tablename="tmp_macview_info_infosys_add_fiveminute";

        SparkSession session = JavaSparkUtil.getRemoteSparkSession("", "parquet_table");
        List<String> datelist = getPartitionDate(tdate);


        Dataset<Row> mac_filter_df  = session.sql("select a.mac as filtermac  from (select a.time time,a.mac mac,sum(a.ct) pv,max(a.ct2) hour from " +
                " (select time,devid,mac,count(scan_time) ct,count(distinct substr(scan_time,12,2)) ct2 from parquet_table.mac_scan_log_mac_cleaned where time>='" + datelist.get(datelist.size() - 1) + "' and time<='" + datelist.get(0) + "'  " +
                " group by time,devid,mac) as a  group by a.time,a.mac) as a where a.pv>" + num_filter + " or a.hour>" + hour_filter + "  group by mac");
        mac_filter_df.createOrReplaceTempView("tmp_mac_filter");
        mac_filter_df.persist(StorageLevel.MEMORY_AND_DISK());
        session.sql("select * from tmp_mac_filter").show();

        if(run_type.equals("day")){
            generateResultInfo(session,tdate,minute,tablename);
        }else if(run_type.equals("flow")){
            for(String partition_date:datelist){
                generateResultInfo(session,partition_date,minute,tablename);
            }
        }
        session.stop();
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


    public static void  generateResultInfo(SparkSession session,String tdate,int seconds,String tablename){


        Dataset<Row> src_mac_scan = session.sql(" select k.devid,k.mac,k.scan_time from (" +
                " select m.*,f.filtermac from " +
                "  (select * from parquet_table.mac_scan_log_mac_cleaned where time='" + tdate + "' and substr(scan_time,1,10)='" + tdate.replace("_", "-") + "' and sch_version!='0') m " +
                "   left join " +
                "   tmp_mac_filter f  on m.mac=f.filtermac" +
                " ) k where k.filtermac is null");
        src_mac_scan.createOrReplaceTempView("tmp_src_macscan");

        //获取到达两次或者两次以上的devid,mac,time记录
        Dataset<Row> base_data = session.sql("select devid,mac,scan_time,pre_scan_time, scan_new,pre_new from " +
                " (" +
                "  select devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "   (" +
                "     select devid,mac,scan_time,lag(scan_time,1) over ( partition by devid,mac order by scan_time) as pre_scan_time" +
                "       from tmp_src_macscan  " +
                "    )  t " +
                " ) p where scan_new-pre_new>=" + seconds + " ");

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
                                //100  50
                                //170 110
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
                "     from tmp_src_macscan " +
                "  ) t " +
                " )" +
                " ) group by devid,mac having sum(flag)=0");

        Dataset<Row> all_devidmac = dataFrame.union(one_reach);

        all_devidmac.createOrReplaceTempView("tmp_expose");


        //保存表数据
        saveTwoMinuteTable(session,tablename,tdate,"parquet_table");

    }

    //保存生成的表
    public static void saveTwoMinuteTable(SparkSession session,String tablename,String tdate,String dblocation){
        session.sql("CREATE  TABLE IF NOT EXISTS db01."+tablename+" (" +
                "devid string," +
                "mac string," +
                "scan_time string )" +
                " PARTITIONED BY (time string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " );

        Dataset<Row> result_dataset = session.sql("select devid,mac,scan_time from tmp_expose");
        result_dataset.repartition(80).write().mode(SaveMode.Overwrite).parquet("/user/yarn/fiveminute/"+tdate);
        session.sql(" load data inpath '/user/yarn/fiveminute/"+tdate+"/*.parquet' overwrite into table "+tablename+" partition (time='"+tdate+"')");
       /* session.sql("insert overwrite table "+tablename+" partition(time='" + tdate.replace("_","-") + "') select " +
                "devid,mac,scan_time from tmp_expose");*/
    }

}
