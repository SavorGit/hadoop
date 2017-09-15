package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.boxTimeIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-13 下午 2:02.
 */
public class BoxTimeIndexMain {
    public static void main(String[] args) throws IOException {
//        args = new String[]{
//                "hBaseTableSource=box_log",
//                "hBaseTable=box_time_index",
//                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/TEST",
//        };
//        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");

        if(args.length < 3){
            throw new IOException("please write hBaseTableSource, hBaseTable and hdfsOutPath...");
        }
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new BoxTimeIndexScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
