package com.littlehotspot.hadoop.mr.probe.utils;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;

/**
 *@Author 刘飞飞
 *@Date 2017/8/10 14:45
 */
public class ProbeUtils {
    public static void main(String[] args) {
//        File file=new File("E:\\data\\probe");
//        File[] files=file.listFiles();
//        for(File f:files){
//            unGzipFile(f);
//        }
        System.out.println(toTime("20171112"));
    }

    public static void unGzipFile(File file) {
        File out=new File(file.getParentFile().getAbsolutePath()+"/file");
        if(!out.exists()){
            out.mkdirs();
        }
        String ouputfile =file.getParentFile().getAbsolutePath()+"/file/"+file.getName();
        try {
            //建立gzip压缩文件输入流
            FileInputStream fin = new FileInputStream(file);
            //建立gzip解压工作流
            GZIPInputStream gzin = new GZIPInputStream(fin);
            //建立解压文件输出流
            FileOutputStream fout = new FileOutputStream(ouputfile);
            int num;
            byte[] buf=new byte[1024];
            while ((num = gzin.read(buf,0,buf.length)) != -1)
            {
                fout.write(buf,0,num);
            }
            gzin.close();
            fout.close();
            fin.close();
        } catch (Exception ex){
            System.err.println(ex.toString());
        }
        return;
    }

    public static String toJoin(String split,String... args){
        if(args==null || args.length==0){
            return "";
        }
        return StringUtils.join(args,split);
    }
    static SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat ymdFormat=new SimpleDateFormat("yyyyMMdd");
    public static String toDate(String time){
        return format.format(new Date(Long.parseLong(time)));
    }

    public static String toDateYMD(String time){
        return ymdFormat.format(new Date(Long.parseLong(time)));
    }

    public static String toTime(String date){
        try {
            return Long.toString(ymdFormat.parse(date).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}
