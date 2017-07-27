package com.littlehotspot.hadoop.mr.box.main;

import com.littlehotspot.hadoop.mr.box.scheduler.BoxIntegratedScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class BoxJdbcDataMain {
    public static void main(String[] args) throws Exception{

        if (args.length < 3) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new BoxIntegratedScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
