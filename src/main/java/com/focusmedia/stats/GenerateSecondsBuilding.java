package com.focusmedia.stats;

import com.focusmedia.bean.BuildDevTime;
import com.focusmedia.bean.TimeBean;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
* 统计每个楼上面的每个设备对应的mac数和去重后 的mac数量，以及人脸数量
*
* */
public class GenerateSecondsBuilding {

    public static void main(String[] args){
        String appname=args[0];
        //对应日期
        String filename=args[1];

        //过滤调超过指定扫描记录数的mac记录
        int flag_times=Integer.parseInt(args[2]);

        //表名后缀
        String suffix_table=args[3];

        List<String> result_list=new ArrayList<>();
        SparkConf conf=new SparkConf();
        conf.setAppName(appname);
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","800");
        conf.set("spark.sql.crossJoin.enabled","true");
        conf.set("spark.sql.shuffle.partitions", "800");
        //conf.set("spark.memory.storageFraction","0.5"); //设置storage占spark.memory.fraction的占比
        // conf.set("spark.memory.fraction","0.6");    //设置storage和execution执行内存，调低它会增加shuffle 溢出的几率，但是会增加用户能使用的自定义数据，对象的空间
        conf.set("spark.shuffle.sort.bypassMergeThreshold","800");
        conf.set("spark.locality.wait","20s");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        session.sql("use parquet_table");
        Map<String, List<String>> build_devidlist = getExcelDateTime(filename);
        for(Map.Entry<String,List<String>> entry:build_devidlist.entrySet()){
            // split[0] 代表大厦名称
            //split[1] 代表2018_09_10-2018_09_15
            String[] split = entry.getKey().split(",");
            List<String> devid_list = entry.getValue();
            List<String> runTdate = getRunTdate(split[1]);
            StringBuffer buffer=new StringBuffer();
            for(String devid:devid_list){
                buffer.append("devid='"+devid+"' or ");
            }
            //解析为 devid='002000003376' or devid='002000003377'
            String substring = buffer.substring(0, buffer.length() - 3);
            for(String tdate:runTdate){
                System.out.println("开始跑:"+split[0]+","+tdate +" |" +substring.toString());
                String result = generateTimesResult(session, split[0],tdate, substring.toString(), flag_times);
                System.out.println("生成的数据格式为："+result);
                result_list.add(result);
            }
        }

        JavaRDD<String> parallelize = context.parallelize(result_list, 1);
        parallelize.saveAsTextFile("/usr/"+suffix_table);
        session.stop();
    }


    public static String generateTimesResult(SparkSession session,String buildname,String tdate,String devid,int flag_times){

        //取出当天的face_log的相关记录

        Dataset<Row> face_data = session.sql("select logid,devid,starttime from face_log where " +
                " time = '" + tdate + "' " +
                " and ( "+ devid +" ) ");
        face_data.createOrReplaceTempView("tmp_face_table");
        face_data.persist(StorageLevel.MEMORY_ONLY());
        //session.sql("select * from tmp_face_table").show(3);
        //取出那些单个设备下扫描记录数大于指定条数的mac数据
        Dataset<Row> exclude_mac_data = session.sql("" +
                " select devid,mac,count(*) as nums from mac_scan_log where " +
                " time='" + tdate + "' " +
                " and ( "+devid+" ) " +
                " group by devid,mac" +
                " having count(*) > " + flag_times);
        exclude_mac_data.createOrReplaceTempView("tmp_exclude_macscanlog");

        //剔除掉上面所选取的记录,也就是没有nums的记录
        Dataset<Row> src_macdata = session.sql(" select m.devid,m.mac,m.scan_time from ( " +
                " select * from mac_scan_log where " +
                " time='" + tdate + "'" +
                " and ( "+devid+" ) " +
                " ) m left join tmp_exclude_macscanlog e" +
                " on m.devid=e.devid and m.mac=e.mac " +
                " where e.nums is null");
        src_macdata.createOrReplaceTempView("tmp_src_maclog");

        //最后对数据进行去伪来获取原始数据表
        Dataset<Row> exclude_fake_mac = session.sql(" " +
                " select m.* from tmp_src_maclog m " +
                " inner join db01.mac_factory  f" +
                " on lower(substr(m.mac,1,6)) = lower(f.mac_index_factory)");
        exclude_fake_mac.createOrReplaceTempView("tmp_exclude_macscan");
        exclude_fake_mac.persist(StorageLevel.MEMORY_ONLY());
        //session.sql("select * from tmp_exclude_macscan").show(3);

        //分别获取+1,-1,+-1秒.....,一直到+15,-15,+-15秒的统计mac数
        StringBuffer buffer=new StringBuffer();
        buffer.append(buildname+","+tdate+",");
        //获取双向 + - 的mac 的不去重数 和去重数
        Dataset<Row> double_macresult = session.sql("select count(*) as double_allnums,count(distinct mac) as double_distinctnums" +
                    "  from tmp_exclude_macscan ");
        List<Row> double_macresult_list = double_macresult.javaRDD().collect();

        Dataset<Row> face_double = session.sql("select count(*) from tmp_face_table where " +
                    " ");
        List<Row> face_double_list = face_double.javaRDD().collect();
        buffer.append(double_macresult_list.get(0).get(0).toString()+","+
                    double_macresult_list.get(0).get(1).toString()+","+
                    face_double_list.get(0).get(0).toString()+",");

        String result_sub = buffer.substring(0, buffer.length() - 1);

        //insertTimeResultData(session,tdate,minute,"summer_result_"+table_suffix,devid,buffer.toString());
        face_data.unpersist();
        exclude_fake_mac.unpersist();
        return result_sub;
    }


    public static List<String> getRunTdate(String mydate){
        SimpleDateFormat df=new SimpleDateFormat("yyyy_MM_dd");
        String[] split = mydate.split("-");
        List<String> result=new ArrayList<>();
        try {
            Date start=df.parse(split[0]);
            Date end=df.parse(split[1]);
            Calendar calendar=Calendar.getInstance();
            calendar.setTime(start);
            while(calendar.getTime().before(end)){
                result.add(df.format(calendar.getTime()));
                calendar.add(Calendar.DAY_OF_MONTH,1);
            }
            result.add(df.format(end));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }


    //读取excel中，获取相应的需要进行数据统计的日期和时间，
    public static Map<String,List<String>> getExcelDateTime(String excelfile){
        Map<String,List<String>> result = new HashMap<>();
        File excel=new File(excelfile);
        FileInputStream in = null;
        try {
            in = new FileInputStream(excel);
            Workbook wb=new XSSFWorkbook(in);
            Iterator<Sheet> sheetIterator = wb.sheetIterator();
            while(sheetIterator.hasNext()){
                Sheet cur_sheet = sheetIterator.next();
                System.out.println(cur_sheet.getLastRowNum());
                Iterator<org.apache.poi.ss.usermodel.Row> rowIterator = cur_sheet.rowIterator();
                int index=0;
                while(rowIterator.hasNext()){
                    org.apache.poi.ss.usermodel.Row excel_row = rowIterator.next();
                    if(index==0){
                        index++;
                        continue;
                    }else{
                        if(excel_row.getCell(0) == null){
                            continue;
                        }
                        String time = excel_row.getCell(0).getStringCellValue();
                        String devid = excel_row.getCell(1).getStringCellValue();
                        String buildname = excel_row.getCell(2).getStringCellValue();
                        if(result.get(buildname+","+time) ==null || result.get(buildname+","+time).size()==0){
                            List<String> mybuildlist=new ArrayList<>();
                            mybuildlist.add(devid);
                            result.put(buildname+","+time,mybuildlist);
                        }else{
                            result.get(buildname+","+time).add(devid);
                        }
                    }
                    index++;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static void insertTimeResultData(SparkSession session,String tdate,String minute,String tablename,String devid,String value){
        String mytime=tdate.replace("_","-")+" "+minute;
        session.sql("CREATE  TABLE IF NOT EXISTS "+tablename+" (" +
                "devid string," +
                "flag_time string, " +
                "result_str string )" +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " );

        session.sql("insert into table "+tablename+" values ('"+devid+"' , '"+mytime+"', '"+value+"')");
    }


    public static String getParHourDate(String date,int flag){
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parse = null;
        try {
            parse = simpleDateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(parse);
        calendar.add(Calendar.SECOND,flag);
        return simpleDateFormat.format(calendar.getTime());
    }

}
