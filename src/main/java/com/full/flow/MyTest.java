package com.full.flow;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class MyTest {


    public static void main(String[] args) throws ParseException {
        SparkConf conf=new SparkConf();
        conf.setMaster("local[*]");
        //conf.set("spark.scheduler.listenerbus.eventqueue.size","50000");
        conf.set("spark.sql.warehouse.dir", "spark-warehouse");
        conf.set("spark.default.parallelism","800");

        //conf.set("spark.speculation","true");
        //conf.set("spark.speculation.interval","6000ms");
        //conf.set("spark.memory.offHeap.enabled","true");
        //conf.set("spark.memory.offHeap.size","512000");
        JavaSparkContext context=new JavaSparkContext(conf);
        context.setLogLevel("ERROR");
        List<Tuple2<String,String>> first = new ArrayList<Tuple2<String,String>>();
        first.add(new Tuple2<>("Json","Shanghai"));
        first.add(new Tuple2<>("Oliver","Beijing"));
        JavaPairRDD<String, String> firstrdd = context.parallelizePairs(first);

        List<Tuple2<String,String>> second = new ArrayList<Tuple2<String,String>>();
        second.add(new Tuple2<>("Json","THis is my name"));
        second.add(new Tuple2<>("Oliver","hello world"));
        JavaPairRDD<String, String> secondrdd = context.parallelizePairs(second);

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = firstrdd.cogroup(secondrdd);

        List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> take = cogroup.take(3);
        for(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> each : take){
            System.out.println(each);
        }
        JavaPairRDD<String, String> updated = cogroup.flatMapValues(new Function<Tuple2<Iterable<String>, Iterable<String>>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Tuple2<Iterable<String>, Iterable<String>> grouped) throws Exception {
                ArrayList<String> first = Lists.newArrayList(grouped._1);
                first.addAll(Lists.newArrayList(grouped._2));
                return first;
            }
        });
        List<Tuple2<String, String>> collect = updated.collect();
        for(Tuple2<String, String> me : collect){
            System.out.println(me);
        }
        context.close();

    }
}