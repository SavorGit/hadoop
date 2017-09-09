package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-04 下午 9:33.
 */
public class NgContentLogToHdfsMain {
    public static void main(String[] args) throws IOException {
//        args = new String[]{
//                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/nginx_content_source/2017-09-04",
//                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/nginx_content_export/test",
//        };
//        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");

        if(args.length < 2){
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new NgContentLogToHdfs(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
