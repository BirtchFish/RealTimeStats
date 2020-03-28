package com.focusmedia.newgenerate;

import com.google.common.collect.Lists;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisClusterCRC16;

import java.util.*;

public class RedisUtils {
	private final static JedisPoolConfig jedisPoolConfig;
	private static JedisCluster cluster;
	private final static Set<HostAndPort> nodesSet;
	private final static int total=Integer.parseInt(ParseProperties.getProperties("redis_maxactive","10"));
    private final static int maxwait=Integer.parseInt(ParseProperties.getProperties("redis_maxwait","100000"));
    private final static int redis_maxidle=Integer.parseInt(ParseProperties.getProperties("redis_maxidle","10"));
    private final static int redis_conncet_timeout=Integer.parseInt(ParseProperties.getProperties("redis_conncet_timeout","6000"));
	static{
		nodesSet=new HashSet<>();
		jedisPoolConfig= new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(total);
		jedisPoolConfig.setMaxWaitMillis(maxwait);
		jedisPoolConfig.setMaxIdle(redis_maxidle);
		String []nodes=ParseProperties.getProperties("redis_cluster").split(",");
		for(String node:nodes){
			nodesSet.add(new HostAndPort(node.split(":")[0],Integer.parseInt(node.split(":")[1])));
		}
		cluster=new JedisCluster(nodesSet,redis_conncet_timeout,jedisPoolConfig);
	}
	public static JedisCluster getInstance(){
		if(null==cluster){
			cluster=new JedisCluster(nodesSet,redis_conncet_timeout,jedisPoolConfig);
		}
		return cluster;
	}
	public static void listPut (String key,Map<String,String> map){
		Long length=cluster.llen(key);
		length++;
	}
	public static TreeSet<String> keys(String pattern) throws Exception{  
		TreeSet<String> keys = new TreeSet<>();  
		Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();  
		for(String k : clusterNodes.keySet()){  
			JedisPool jp = clusterNodes.get(k);  
			Jedis connection = jp.getResource();  
			try {
				//keys.addAll(connection.hkeys(pattern));  
				keys.addAll(connection.keys(pattern));  
			} catch(Exception e){  
				throw new Exception("Getting keys error: {}", e);
			} finally{  
				connection.close();
			}  
		} 
		return keys;  
	}

/*	public static Jedis getSingleClient(){
		Jedis client=new Jedis("172.19.100.44",7002,600000);
		client.select(0);
		return client;
	}

	public static void flushsingle(){
		Jedis client=new Jedis("172.19.100.44",7002,600000);
		client.select(0);
		client.flushDB();
		client.close();
	}*/

	public static void flushall(){

		JedisCluster instance = getInstance();
		Map<String, JedisPool> clusterNodes = instance.getClusterNodes();
		for(Map.Entry<String,JedisPool> entry:clusterNodes.entrySet()){
			String host_port = entry.getKey();
			JedisPool value = entry.getValue();
			Jedis resource = value.getResource();
			try{
				resource.select(0);
				resource.flushDB();
				value.returnResource(resource);
			}catch (JedisDataException e){
				System.out.println("无法在slave上进行flushdb操作"+host_port);
			}finally {

			}
		}
	}



	public static void delKeys(String keysPattern) {  
		Map<String, JedisPool> clusterNodes =cluster.getClusterNodes();  
		for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {  
			Jedis jedis = entry.getValue().getResource();  
			if (!jedis.info("replication").contains("role:slave")) {  
				Set<String> keys = jedis.keys(keysPattern);  
				if (keys.size() > 0) {  
					Map<Integer, List<String>> map = new HashMap<>(6600);  
					for (String key : keys) {  
						int slot = JedisClusterCRC16.getSlot(key);
						if (map.containsKey(slot)) {  
							map.get(slot).add(key);  
						} else {  
							map.put(slot, Lists.newArrayList(key));
						}  
					}  
					for (Map.Entry<Integer, List<String>> integerListEntry : map.entrySet()) {  
						jedis.del(integerListEntry.getValue().toArray(new String[integerListEntry.getValue().size()]));  
					}  
				}  
			}  
		}  
	}  
	public static void main(String[] args) throws Exception {
		
		RedisUtils.getInstance();
		Iterator<String> iterbu1=keys("*254438*").iterator();
		while(iterbu1.hasNext()){
			String key=iterbu1.next();
			System.out.println("--------"+key);
			Map<String,String> map=RedisUtils.cluster.hgetAll(key);
			System.out.println("+++++++++++"+map);
		}
		
		
		
		
	
	}

}
