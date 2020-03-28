package com.focusmedia.stats;

import com.focusmedia.newgenerate.GenerateNewMacScanInfo2;
import com.focusmedia.newgenerate.MacResultBean;
import com.focusmedia.newgenerate.MacScanVo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Test {

    public static void main(String[] args) throws ParseException {

        TreeMap<Long,String> test = new TreeMap<>();
        test.put(1l,"one");
        test.put(3l,"one");
        test.put(2l,"one");
        test.put(5l,"one");
        test.put(4l,"one");
        Object[] objects = test.keySet().toArray();
        for(int i=0;i<objects.length;i++){
            System.out.println(objects[i]);
        }
    }

}
