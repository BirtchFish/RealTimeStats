package com.focusmedia.stats;

import com.focusmedia.bean.TimeBean;
import com.focusmedia.util.JavaSparkUtil;
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
* 统计对应设备上的+1,-1,+-1, +2,-2,+-2,........+15,-15,+-15秒情况下的mas扫描情况
*
* */
public class GenerateSecondsDev {

    public static void main(String[] args){
        String appname=args[0];

        //对应日期
        String filename=args[1];

        //过滤调超过指定扫描记录数的mac记录
        int flag_times=Integer.parseInt(args[2]);

        //指定要扫描到的设备
        String devid=args[3];


        //表名后缀
        String suffix_table=args[4];

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
        List<TimeBean> excelDateTime = getExcelDateTime(filename, new SimpleDateFormat("HH:mm:ss"),new SimpleDateFormat("yyyy_MM_dd"));
        for(TimeBean bean:excelDateTime){
            System.out.println("开始执行："+bean.toString());
            String result = generateTimesResult(bean.time, session, bean.date, devid, flag_times);
            System.out.println("生成的数据格式为："+result);
            result_list.add(result);
        }

        JavaRDD<String> parallelize = context.parallelize(result_list, 1);
        parallelize.saveAsTextFile("/usr/"+suffix_table);
        session.stop();
    }


    public static String generateTimesResult(String minute,SparkSession session,String tdate,String devid,int flag_times){

        //取出当天的face_log的相关记录

        Dataset<Row> face_data = session.sql("select logid,devid,starttime from face_log where " +
                " time = '" + tdate + "' " +
                " and (devid='" + devid + "' or ''='" + devid + "' ) ");
        face_data.createOrReplaceTempView("tmp_face_table");
        face_data.persist(StorageLevel.MEMORY_ONLY());
        session.sql("select * from tmp_face_table").show(3);
        //取出那些单个设备下扫描记录数大于指定条数的mac数据
        Dataset<Row> exclude_mac_data = session.sql("" +
                " select devid,mac,count(*) as nums from mac_scan_log where " +
                " time='" + tdate + "' " +
                " and (devid='" + devid + "' or ''='" + devid + "' ) " +
                " group by devid,mac" +
                " having count(*) > " + flag_times);
        exclude_mac_data.createOrReplaceTempView("tmp_exclude_macscanlog");

        //剔除掉上面所选取的记录,也就是没有nums的记录
        Dataset<Row> src_macdata = session.sql(" select m.devid,m.mac,m.scan_time from ( " +
                " select * from mac_scan_log where " +
                " time='" + tdate + "'" +
                " and (devid='" + devid + "' or ''='" + devid + "' ) " +
                " ) m left join tmp_exclude_macscanlog e" +
                " on m.devid=e.devid and m.mac=e.mac " +
                " where e.nums is null");
        src_macdata.createOrReplaceTempView("tmp_src_maclog");
        //session.sql("select * from tmp_src_maclog").show();
        src_macdata.persist(StorageLevel.MEMORY_ONLY());
        session.sql("select * from tmp_src_maclog").show(3);

        //分别获取+1,-1,+-1秒.....,一直到+15,-15,+-15秒的统计mac数
        StringBuffer buffer=new StringBuffer();
        buffer.append(minute+",");
        buffer.append(devid+",");
        for(int i=10;i<=15;i=i+5){

            //获取当前的指定的日期数据
            String current_time = getParHourDate(tdate.replace("_","-")+" "+minute,0);
            System.out.println("当前时间："+current_time);
            //获取向上+ 的时间
            String up_time = getParHourDate(tdate.replace("_","-")+" "+minute,i);
            System.out.println("向上时间："+up_time);
            // 获取向下- 的时间
            String down_time = getParHourDate(tdate.replace("_","-")+" "+minute,-i);
            System.out.println("向下时间："+down_time);

            /*//获取向上的mac 的不去重数 和去重数
            Dataset<Row> up_macresult = session.sql("select count(*) as up_allnums,count(distinct mac) as up_distinctnums" +
                    "  from tmp_src_maclog where scan_time <='" + up_time + "' and scan_time >='" + current_time + "' ");
            List<Row> up_macresult_list =up_macresult.javaRDD().collect();


            Dataset<Row> face_up = session.sql("select count(*) from tmp_face_table where " +
                    " starttime <='" + up_time + "' and starttime >='" + current_time + "'");
            List<Row> face_up_list = face_up.javaRDD().collect();
            buffer.append(up_macresult_list.get(0).get(0).toString()+"|"+
                    up_macresult_list.get(0).get(1).toString()+"|"+
                    face_up_list.get(0).get(0).toString()+",");

            //获取向下的mac 的不去重数 和去重数
            Dataset<Row> down_macresult = session.sql("select count(*) as down_allnums,count(distinct mac) as down_distinctnums" +
                    "  from tmp_src_maclog where scan_time <='" + current_time + "' and scan_time >='" + down_time + "' ");
            List<Row> down_macresult_list = down_macresult.javaRDD().collect();

            Dataset<Row> face_down = session.sql("select count(*) from tmp_face_table where " +
                    " starttime <='" + current_time + "' and starttime >='" + down_time + "'");
            List<Row> face_down_list = face_down.javaRDD().collect();
            buffer.append(down_macresult_list.get(0).get(0).toString()+"|"+
                    down_macresult_list.get(0).get(1).toString()+"|"+
                    face_down_list.get(0).get(0).toString()+",");*/

            //获取双向 + - 的mac 的不去重数 和去重数
            Dataset<Row> double_macresult = session.sql("select count(*) as double_allnums,count(distinct mac) as double_distinctnums" +
                    "  from tmp_src_maclog where scan_time <='" + up_time + "' and scan_time >='" + down_time + "' ");
            List<Row> double_macresult_list = double_macresult.javaRDD().collect();

            Dataset<Row> face_double = session.sql("select count(*) from tmp_face_table where " +
                    " starttime <='" + up_time + "' and starttime >='" + down_time + "'");
            List<Row> face_double_list = face_double.javaRDD().collect();
            buffer.append(double_macresult_list.get(0).get(0).toString()+"|"+
                    double_macresult_list.get(0).get(1).toString()+"|"+
                    face_double_list.get(0).get(0).toString()+",");
        }
        String result_sub = buffer.substring(0, buffer.length() - 1);

        //insertTimeResultData(session,tdate,minute,"summer_result_"+table_suffix,devid,buffer.toString());
        face_data.unpersist();
        src_macdata.unpersist();
        return result_sub;
    }

    //读取excel中，获取相应的需要进行数据统计的日期和时间，
    public static List<TimeBean> getExcelDateTime(String excelfile,SimpleDateFormat sdf,SimpleDateFormat datesdf){
        List<TimeBean> timeBeans=new ArrayList<>();
        File excel=new File(excelfile);
        FileInputStream in = null;
        try {
            in = new FileInputStream(excel);
            Workbook wb=new XSSFWorkbook(in);
            Iterator<Sheet> sheetIterator = wb.sheetIterator();
            while(sheetIterator.hasNext()){
                Sheet cur_sheet = sheetIterator.next();
                Iterator<org.apache.poi.ss.usermodel.Row> rowIterator = cur_sheet.rowIterator();
                int index=0;
                while(rowIterator.hasNext()){
                    org.apache.poi.ss.usermodel.Row excel_row = rowIterator.next();
                    if(index==0){
                        index++;
                        continue;
                    }else{
                        String date = datesdf.format(datesdf.parse(excel_row.getCell(2).toString().replace(".","_")));
                        String  mytime=sdf.format(excel_row.getCell(6).getDateCellValue());
                        timeBeans.add(new TimeBean(date,mytime));
                    }
                    index++;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timeBeans;
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
