package com.full.flow;

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

import java.util.*;
import java.util.function.Consumer;

/*
*基于全国的粒度
* 基于two minute的方式来生成对应 , 计算1+，2+，3+ ........
* */
public class GrpOneAndMore {
    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    //计算1+，2+.........., 3+..............
    public static void  generateResultInfo(SparkSession session,String tdate,long seconds){

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


        Dataset<Row> fake_mac_view_info = session.sql(" select t.* from  " +
                "   (select *,substr(mac,0,6) as submac from mac_view_info where time='" + tdate + "') t" +
                "    left join " +
                "    db01.mac_factory as p" +
                "    on t.submac=p.mac_index_factory" +
                "     where factory_name is not null ");
        fake_mac_view_info.createOrReplaceTempView("tmp_fake_macview");

        //获取到达两次或者两次以上的devid,mac,time记录
        Dataset<Row> base_data = session.sql("select devid,mac,scan_time,pre_scan_time, scan_new,pre_new from " +
                " (" +
                "  select devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "   (" +
                "     select devid,mac,scan_time,lag(scan_time,1) over ( partition by devid,mac order by scan_time) as pre_scan_time" +
                "       from tmp_fake_macview "+
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
        dataFrame.createOrReplaceTempView("tmp_moretwo");

        //获取大于等于2次以上曝光的人的数量
        Dataset<Row> two_expose_nums = session.sql("select expose_time,sum(1) as mac_nums from " +
                " (select devid,mac,count(*) as expose_time from tmp_moretwo group by devid,mac) t" +
                "   group by expose_time");
        List<Row> two_expose_times = two_expose_nums.collectAsList();
        two_expose_times.sort(new Comparator<Row>() {
            @Override
            public int compare(Row o1, Row o2) {
                return Integer.compare(Integer.parseInt(o1.get(0).toString()),Integer.parseInt(o2.get(0).toString()));
            }
        });

        //计算一次到达的那些mac记录

        Dataset<Row> one_reach = session.sql("select devid,mac, max(scan_time) as scan_time from " +
                " (" +
                " select devid,mac,scan_time,case when scan_new-pre_new<" + seconds + " then 0 else 1 end as flag  from " +
                " (" +
                " select devid,mac,scan_time,pre_scan_time,unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') as scan_new, case when pre_scan_time is null then unix_timestamp(scan_time,'yyyy-MM-dd HH:mm:ss') else unix_timestamp(pre_scan_time,'yyyy-MM-dd HH:mm:ss') end as pre_new from " +
                "  (" +
                "    select devid,mac,scan_time,lag(scan_time,1) over ( partition by devid,mac order by scan_time) as pre_scan_time" +
                "     from tmp_fake_macview "+
                "  ) t " +
                " )" +
                " ) group by devid,mac having sum(flag)=0");


        //一次到达的总的mac数
        List<ExposeTime> result=new ArrayList<ExposeTime>();
        long count = one_reach.count();
        long sum_time=0;
        for(int i=1;i<=50;i++){
            if(i==1){
                //代表一次到达的，先把一次到达的先加上去
                sum_time=count;
            }
            for(Row line_row:two_expose_times){
                int expose_type = Integer.parseInt(line_row.get(0).toString());
                if(expose_type>=i){
                    //只要比需要计算 的曝光次数大，则把数量加上去
                    sum_time+=Integer.parseInt(line_row.get(1).toString());
                }
            }
            ExposeTime exposeTime=new ExposeTime(i,sum_time,tdate);
            result.add(exposeTime);
            sum_time=0;
        }

        Dataset<Row> expose_bean = session.createDataFrame(result, ExposeTime.class);
        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        expose_bean.write().mode(SaveMode.Append).jdbc(URL,"ad_grp_times",pro);
    }


    public static void main(String[] args){
        String tdate=args[0];

        SparkSession session = JavaSparkUtil.getRemoteSparkSession("grp_expose", "parquet_table");

        generateResultInfo(session,tdate,150);
        session.stop();
    }
}
