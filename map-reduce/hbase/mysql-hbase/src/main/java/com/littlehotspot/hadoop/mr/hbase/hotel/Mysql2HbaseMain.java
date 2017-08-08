package com.littlehotspot.hadoop.mr.hbase.hotel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Locale;

/**
 * <h1> 酒楼表 </h1>
 * Created by Administrator on 2017-08-08 上午 10:30.
 */
public class Mysql2HbaseMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 9) {
            throw new IOException("please write input path and output path...");
        }

        Locale.setDefault(Locale.CHINA);
        Configuration conf = new Configuration();

        ToolRunner.run(conf, new Mysql2HBaseScheduler(), args);

    }
}
