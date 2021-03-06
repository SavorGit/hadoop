package com.littlehotspot.hadoop.mr.user.tag.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Locale;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-10 下午 1:44.
 */
public class UserTagMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }

        Locale.setDefault(Locale.CHINA);
        Configuration conf = new Configuration();

        ToolRunner.run(conf, new UserTagScheduler(), args);
    }
}
