package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.userDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-16 下午 2:17.
 */
public class UserByDateMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();

        ToolRunner.run(conf, new UserDateScheduler(), args);
    }
}
