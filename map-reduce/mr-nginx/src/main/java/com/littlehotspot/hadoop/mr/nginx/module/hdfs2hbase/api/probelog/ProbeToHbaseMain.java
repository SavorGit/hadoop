package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.probelog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-24 下午 3:35.
 */
public class ProbeToHbaseMain {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ProbeToHbase(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
