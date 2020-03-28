package com.build.transform;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/*
* 读取楼宇Excel 文件,读取每行的数据，并用 | 分割每一列，然后保存到txt文件中
*
* */
public class TransFormExcelToHive  {

    public  static SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");

    public static void generateBuildingData(String path){
        String format = dateFormat.format(new Date());
        File pathfile=new File(path);
        File[] files = pathfile.listFiles();
        ExcelXlsxReader reader=new ExcelXlsxReader();
        try{
            for(File buildfile:files){
                if(buildfile.isFile()){
                    String filename = buildfile.getName();
                    String outpath=path+"/"+format;
                    File outputdir=new File(outpath);
                    if(!outputdir.exists()){
                        outputdir.mkdir();
                    }
                    if(filename.contains("A1")){
                        String outpath_file=path+"/"+format+"/A1.txt";
                        FileOutputStream out=new FileOutputStream(outpath_file);
                        reader.process(buildfile.getAbsolutePath(),out);
                        out.flush();
                        out.close();
                    }else if(filename.contains("A2")){
                        String outpath_file=path+"/"+format+"/A2.txt";
                        FileOutputStream out=new FileOutputStream(outpath_file);
                        reader.process(buildfile.getAbsolutePath(),out);
                        out.flush();
                        out.close();
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        generateBuildingData("e:/excel");
    }

}
