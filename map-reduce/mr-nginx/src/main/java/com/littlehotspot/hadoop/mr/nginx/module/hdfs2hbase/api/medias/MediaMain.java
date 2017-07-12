package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-10 下午 6:14.
 */
public class MediaMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new MediaScheduler(), args);
    }
}
