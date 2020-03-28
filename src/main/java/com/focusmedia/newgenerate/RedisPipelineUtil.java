package com.focusmedia.newgenerate;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.*;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public class RedisPipelineUtil {

    public static JedisCluster instance=null;
    public static Map<String, JedisPool> clusterNodes=null;
    public static TreeMap<Long, String> slotHostMap=null;

    static{

        instance = RedisUtils.getInstance();
        //地址:port 和 jredis pool的对应关系
        clusterNodes = instance.getClusterNodes();
        Iterator<String> iterator = clusterNodes.keySet().iterator();

        //槽号和 地址:port的对应关系
        slotHostMap = getSlotHostMap(iterator.next());

    }

    //槽号和 地址:port的对应关系
    public static TreeMap<Long, String> getSlotHostMap(String anyHostAndPortStr) {
        TreeMap<Long, String> tree = new TreeMap<Long, String>();
        String parts[] = anyHostAndPortStr.split(":");
        HostAndPort anyHostAndPort = new HostAndPort(parts[0], Integer.parseInt(parts[1]));
        try{
            Jedis jedis = new Jedis(anyHostAndPort.getHost(), anyHostAndPort.getPort());
            List<Object> list = jedis.clusterSlots();
            for (Object object : list) {
                List<Object> list1 = (List<Object>) object;
                List<Object> master = (List<Object>) list1.get(2);
                String hostAndPort = new String((byte[]) master.get(0)) + ":" + master.get(1);
                tree.put((Long) list1.get(0), hostAndPort);
                tree.put((Long) list1.get(1), hostAndPort);
            }
            jedis.close();
        }catch(Exception e){

        }
        return tree;
    }

    //获取对应相同redis节点下的所有的key
    public static String getKeysSlots(String key,TreeMap<Long, String> slotHostMap){
            int slot = JedisClusterCRC16.getSlot(key);
            //获取满足slot区间的对应的 地址:port
            if(slotHostMap.containsKey((long)slot)){
                //在获取对应这个key的 主机:port
                String hostport = slotHostMap.get((long)slot);
                return hostport;
            }else{
                //取出小于该key的第一个key:value键值对
                Map.Entry<Long, String> longStringEntry = slotHostMap.lowerEntry((long) slot);
                String hostport = longStringEntry.getValue();
                return hostport;
            }

    }

  /*  //根据主机：地址-keys， 和主机地址：jredisPool来进行批量查值
    public static void makeBatchKeyValue(Map<String, List<String>> keysSlots, Map<String, JedisPool> clusterNodes,
                                         SparkSession session, String hivetablename, String hdfspath, String[] sqlArr){

       *//* Timer timer=new Timer();
        timer.schedule(new MyTimerTask(result),10000,5000);*//*
        Iterator<Map.Entry<String, List<String>>> iterator = keysSlots.entrySet().iterator();
        while(iterator.hasNext()){

            List<Object> result=new ArrayList<Object>();

            Map.Entry<String, List<String>> next = iterator.next();
            //主机:port
            String hostport = next.getKey();
            //所有的key
            List<String> keys = next.getValue();
            Jedis client = clusterNodes.get(hostport).getResource();
            HashMap<byte[], Response<Map<byte[],byte[]>>> newMap=new HashMap<byte[], Response<Map<byte[],byte[]>>>();

            Pipeline pipelined = client.pipelined();
            int tmp=0;

            //对这台主机下的所有的key进行遍历
            for(int i=0;i<keys.size();i++){

                tmp++;
                newMap.put(keys.get(i).getBytes(),pipelined.hgetAll(keys.get(i).getBytes()));
                if(tmp%10000==0){
                    pipelined.sync();

                    Iterator<Map.Entry<byte[], Response<Map<byte[], byte[]>>>> key_result = newMap.entrySet().iterator();
                    while(key_result.hasNext()){
                        Map.Entry<byte[], Response<Map<byte[], byte[]>>> key_value =key_result.next();
                        String sch_key=new String(key_value.getKey());
                        String[] arrLine = sch_key.split("_");
                        Map<String, String> map = generateStrMap(key_value.getValue().get());
                        map.put("CityCode",arrLine[1]);
                        map.put("BuildingListDate",arrLine[3]);
                        map.put("UploadCount",arrLine[6]);
                        result.add(mapToObject(map, ScheduleScreenFAMByDay.class));
                    }
                    pipelined.clear();
                    tmp=0;
                    newMap.clear();
                }else if(i>=keys.size()-1){
                    //代表已经到最后 了
                    pipelined.sync();
                    Iterator<Map.Entry<byte[], Response<Map<byte[], byte[]>>>> key_result = newMap.entrySet().iterator();
                    while(key_result.hasNext()){
                        Map.Entry<byte[], Response<Map<byte[], byte[]>>> key_value =key_result.next();
                        String sch_key=new String(key_value.getKey());
                        String[] arrLine = sch_key.split("_");
                        Map<String, String> map = generateStrMap(key_value.getValue().get());
                        map.put("CityCode",arrLine[1]);
                        map.put("BuildingListDate",arrLine[3]);
                        map.put("UploadCount",arrLine[6]);
                        result.add(mapToObject(map, ScheduleScreenFAMByDay.class));
                    }
                    pipelined.clear();
                    tmp=0;
                    newMap.clear();
                }

                if(result.size()>=500000){
                    System.out.println("将"+hostport+"数据:"+result.size()+"保存到hdfs中");
                    Dataset<Row> dataSet = session.createDataFrame(result, ScheduleScreenFAMByDay.class);
                    dataSet.repartition(1).write().mode(SaveMode.Append).parquet(hdfspath);
                    result.clear();
                }else if(i>=keys.size()-1 && result.size()<=500000){
                    System.out.println("将"+hostport+"最后剩余数据:"+result.size()+"保存到hdfs中");
                    Dataset<Row> dataSet = session.createDataFrame(result, ScheduleScreenFAMByDay.class);
                    dataSet.repartition(1).write().mode(SaveMode.Append).parquet(hdfspath);
                    result.clear();
                }
            }

            try {
                pipelined.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            client.close();
        }
        for(String sql:sqlArr){
            session.sql(sql);
        }
    }
*/

    public static Map<String,String> generateStrMap(Map<byte[], byte[]> map){
        Map<String,String> result=new HashMap<String,String>();
        Iterator<Map.Entry<byte[], byte[]>> iterator = map.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<byte[], byte[]> next = iterator.next();
            result.put(new String(next.getKey()),new String(next.getValue()));
        }
        return result;
    }

    public static Object mapToObject(Map<String, String> map, Class<?> beanClass){
        if (map == null)
            return null;

        Object obj=null;
        try{
             obj= beanClass.newInstance();
            Field[] fields = obj.getClass().getDeclaredFields();
            for (Field field : fields) {
                int mod = field.getModifiers();
                if(Modifier.isStatic(mod) || Modifier.isFinal(mod)){
                    continue;
                }
                field.setAccessible(true);
               // System.out.println(map.get(field.getName()));
                String field_value = map.get(field.getName());
                if(field_value==null){
                    field.set(obj, null);
                }else{
                    field.set(obj, field_value);
                }

            }
        }catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return obj;
    }



    public static void main(String[] args){

       // System.out.println(1%1);
        System.out.println(RedisPipelineUtil.instance);
        System.out.println(RedisPipelineUtil.instance);
        /*JedisCluster instance = RedisUtils.getInstance();
        //地址:port 和 jredis pool的对应关系
        Map<String, JedisPool> clusterNodes = instance.getClusterNodes();
        Iterator<String> iterator = clusterNodes.keySet().iterator();

        //槽号和 地址:port的对应关系
        TreeMap<Long, String> slotHostMap = getSlotHostMap(iterator.next());

        List<String> testkeys=new ArrayList<String>();
        testkeys.add("sch_CM005001_269481_2018-06-18_11_10_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_9_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_8_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_7_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_6_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_5_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_4_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_3_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_2_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_11_1_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_10_1_3");
        testkeys.add("sch_CM005001_269481_2018-06-18_9_31_3");
        //获取对应地址:port  和所有key的对应关系
        Map<String, List<String>> keysSlots = getKeysSlots(testkeys, slotHostMap);

        getBatchKeyValue(keysSlots,clusterNodes);*/
    }

}
