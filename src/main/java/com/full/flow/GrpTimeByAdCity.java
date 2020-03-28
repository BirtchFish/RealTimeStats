package com.full.flow;

import com.focusmedia.bean.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
*  要基于增补之后的那张表zero，现阶段先用mac_view_info那张表试试
* */
public class GrpTimeByAdCity {
    public static String URL="jdbc:mysql://172.19.100.43:3306/focusmedia_realtime?useUnicode=true&characterEncoding=utf8";
    public static void generateGrpByAdCity(SparkSession session,JavaSparkContext context,String tdate,long seconds){

        //先获取去伪的mac_view_info数据
        Dataset<Row> fake_mac_view_info = session.sql(" select t.city_name,t.devid,t.mac,t.ad_content_id from  " +
                "   (select *,substr(mac,0,6) as submac from mac_view_info where time='" + tdate + "' and ad_content_id is not null and city_name is not null and scan_time is not null ) t" +
                "    left join " +
                "    db01.mac_factory as p" +
                "    on t.submac=p.mac_index_factory" +
                "     where factory_name is not null");
        fake_mac_view_info.createOrReplaceTempView("tmp_fake_macview");
        session.sql("select * from tmp_fake_macview").show();

        //下面城市单独跑
        String[] excludecity=new String[]{"上海","北京","广州","深圳","重庆","南京","成都","天津"};

        //读取 ad_info 获取ad_content_id 和ad_content 的对应关系 数据量不大直接collect
        Dataset<Row> sql = session.sql("select adcontent,adcontent_id from ad_info group by adcontent,adcontent_id");
        List<Row> content_conid = sql.collectAsList();
        final Map<String,String> adcontentid_adcontent_map=new HashMap<String,String>();
        for(Row line:content_conid){
            //用来保存ad_content_id到ad_content的对应关系
            adcontentid_adcontent_map.put(line.get(1).toString(),line.get(0).toString());
        }

        //先跑数据量少的合起来跑
        Dataset<Row> lessdata_city = session.sql("select * from tmp_fake_macview where city_name !='上海' and " +
                " city_name !='北京' and city_name!='广州' and " +
                " city_name!='深圳' and city_name!='重庆' and city_name!='南京' and city_name!='成都' and city_name!='天津'");

        operatorCityGrp(session,context,lessdata_city,adcontentid_adcontent_map,tdate);

        //再对数据量大的城市一个个跑
        for(String city:excludecity){
            Dataset<Row> moredata_city = session.sql("select * from tmp_fake_macview where city_name='" + city + "'");
            operatorCityGrp(session,context,moredata_city,adcontentid_adcontent_map,tdate);
        }
    }


    public static void operatorCityGrp(SparkSession session,JavaSparkContext context,
                                       Dataset<Row> fake_mac_view_info,Map<String,String> adcontentid_adcontent_map,
                                       String tdate
                                       ){
        //Row    :    city_name , devid, mac,  ad_content_id
        Dataset<Row> repartition = fake_mac_view_info.repartition(400);
        JavaRDD<AdMacBean> adMacBeanJavaRDD = repartition.toJavaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>, AdMacBean>() {
            @Override
            public Iterator<AdMacBean> call(Iterator<Row> rowIterator) throws Exception {

                List<AdMacBean> result = new ArrayList<AdMacBean>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    String city_name = row.isNullAt(0) ? null : row.get(0).toString();
                    String devid = row.isNullAt(1) ? null : row.get(1).toString();
                    String mac = row.isNullAt(2) ? null : row.get(2).toString();
                    String ad_content_id = row.isNullAt(3) ? null : row.get(3).toString();
                    AdMacBean bean = new AdMacBean(city_name, devid, mac, ad_content_id, null,tdate);
                    if (ad_content_id != null) {
                        String adcontent = adcontentid_adcontent_map.get(ad_content_id);
                        if (adcontent != null && !adcontent.equals("")) {
                            bean.setAd_content(adcontent);
                        }
                    }
                    result.add(bean);
                }
                return result.iterator();
            }
        });

        Dataset<Row> adMac_set = session.createDataFrame(adMacBeanJavaRDD, AdMacBean.class);
        adMac_set.createOrReplaceTempView("tmp_admac");
        session.sql("select * from tmp_admac").show();


        Dataset<Row> eachAdExpose = session.sql("select city_name,ad_content,expose_times,count(mac) as mactimes from " +
                " (" +
                " select city_name,devid,mac,ad_content,count(*) as expose_times " +
                "   from tmp_admac " +
                "    group by city_name,devid,mac,ad_content" +
                " ) p group by city_name,ad_content,expose_times");
        eachAdExpose.createOrReplaceTempView("tmp_each_expose");
        session.sql("select count(*) from tmp_each_expose").show();



        List<Row> result_rows = eachAdExpose.collectAsList();

        System.out.println("已经Collect完毕..................");
        //城市  和 对应 每条广告 下面的所有记录
        HashMap<String,HashMap<String,List<Row>>> map_result=new HashMap<String,HashMap<String,List<Row>>>();
        for(Row row:result_rows){
            //city_name,  ad_content,  expose_times, mactimes
            String city_name = row.get(0).toString();
            if(map_result.get(city_name)==null){
                HashMap<String,List<Row>> sub_map=new HashMap<String,List<Row>>();
                //取出对应的ad_content
                String ad_content = row.get(1).toString();
                List<Row> inner_ad=new ArrayList<Row>();
                inner_ad.add(row);
                sub_map.put(ad_content,inner_ad);
                map_result.put(city_name,sub_map);
            }else {
                HashMap<String, List<Row>> city_all = map_result.get(city_name);
                if(city_all.get(row.get(1).toString())==null){
                    //代表是没有这个广告对应的下面的记录
                    List<Row> adlist=new ArrayList<Row>();
                    adlist.add(row);
                    map_result.get(city_name).put(row.get(1).toString(),adlist);
                }else{
                    map_result.get(city_name).get(row.get(1).toString()).add(row);
                }
            }
        }

        List<CityAdGrpBean> meresult=new ArrayList<CityAdGrpBean>();
        //对最后的每个城市的每个广告进行1+，2+，3+......进行计算
        for(Map.Entry<String,HashMap<String,List<Row>>> entry:map_result.entrySet()){
            String city_name = entry.getKey();
            HashMap<String, List<Row>> ad_content_rows = entry.getValue();
            for(Map.Entry<String,List<Row>> sub_entry:ad_content_rows.entrySet()){
                String adcontent = sub_entry.getKey();
                List<Row> expose_and_mactimes = sub_entry.getValue();
                //city_name,  ad_content,  expose_times, mactimes
                int sum_time=0;
                for(int i=1;i<=50;i++){
                    sum_time=0;
                    for(Row line:expose_and_mactimes){
                        int expose = Integer.parseInt(line.get(2).toString());
                        if(expose>=i){
                            sum_time+=Integer.parseInt(line.get(3).toString());
                        }
                    }
                    CityAdGrpBean adbean=new CityAdGrpBean(city_name,adcontent,i,sum_time,tdate);
                    meresult.add(adbean);
                    //sum_time=0;
                }
            }
        }

        JavaRDD<CityAdGrpBean> parallelize_result = context.parallelize(meresult);
        Dataset<Row> expose_bean_result = session.createDataFrame(parallelize_result, CityAdGrpBean.class);

        Properties pro=new Properties();
        pro.put("user","root");
        pro.put("password","root");
        pro.put("driver","com.mysql.jdbc.Driver");

        expose_bean_result.write().mode(SaveMode.Append).jdbc(URL,"ad_city_ad_grp_times",pro);

    }

    public static void main(String[] args){
        String tdate=args[0];
        SparkConf conf=new SparkConf();
        conf.setAppName("grp_time_city");
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","700");
        conf.set("spark.sql.crossJoin.enabled","true");
        conf.set("spark.sql.shuffle.partitions", "700");
        conf.set("spark.network.timeout","500");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
        // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
        conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
       // conf.set("spark.sql.shuffle.partitions","700");
        conf.set("spark.locality.wait","20s");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        session.sql("use parquet_table");

        generateGrpByAdCity(session,context,tdate,150);

        session.stop();
    }
}
