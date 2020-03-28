package com.focusmedia.newgenerate;

import java.io.IOException;
import java.util.Properties;

public class ParseProperties {
	private static Properties  properties=null;
	public static Properties getProperties() {
		return properties;
	}
	public static  String getProperties(String key){
		return (String) properties.get(key);
	}
	public static  String getProperties(String key,String defaultValue){
		return (String) properties.getProperty(key, defaultValue);
	}
	static{
		properties=new Properties();
		try {
			properties.load(ParseProperties.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	public static void main(String[] args) throws IOException {
		System.out.println(ParseProperties.getProperties("HADOOP_USER_NAME"));

	}
}
