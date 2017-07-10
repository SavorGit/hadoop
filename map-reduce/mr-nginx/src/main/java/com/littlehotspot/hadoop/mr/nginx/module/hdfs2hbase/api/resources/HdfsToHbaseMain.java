package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 6:10.
 */
public class HdfsToHbaseMain {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ResourceScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
