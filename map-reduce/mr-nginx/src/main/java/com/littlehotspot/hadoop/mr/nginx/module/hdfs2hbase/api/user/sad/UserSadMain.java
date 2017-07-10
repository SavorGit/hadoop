package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-28 下午 3:42.
 */
public class UserSadMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IOException("please write input path and output path...");
        }

        Configuration conf = new Configuration();
        ToolRunner.run(conf, new UserSadScheduler(), args);
    }
}
