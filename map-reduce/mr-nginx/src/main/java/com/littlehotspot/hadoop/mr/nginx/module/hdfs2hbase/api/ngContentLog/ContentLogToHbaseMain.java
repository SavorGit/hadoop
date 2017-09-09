package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-05 下午 2:05.
 */
public class ContentLogToHbaseMain {
    public static void main(String[] args) throws IOException {
//        args = new String[]{
//                "hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/nginx_content_export/test",
//                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/nginx_content_export/testOut",
//        };
//        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");

        if(args.length < 2){
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ContentLogToHbase(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
