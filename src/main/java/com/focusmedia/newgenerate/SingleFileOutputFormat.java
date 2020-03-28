package com.focusmedia.newgenerate;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import java.io.IOException;

public class SingleFileOutputFormat<K,V> extends MultipleTextOutputFormat<K,V> {
    @Override
    protected String generateFileNameForKeyValue(K key, V value, String name) {
        String filename = key.toString();
        return filename;
    }

    @Override
    protected K generateActualKey(K key, V value) {
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws FileAlreadyExistsException, InvalidJobConfException, IOException {
        Path outputPath = FileOutputFormat.getOutputPath(job);
        if(outputPath != null){
            // 代表文件夹存在
            FileOutputFormat.setOutputPath(job,outputPath);
        }else{
            ignored.mkdirs(outputPath);
            FileOutputFormat.setOutputPath(job,outputPath);
        }
    }
}
