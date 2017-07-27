package com.littlehotspot.hadoop.mr.box.main;

import com.littlehotspot.hadoop.mr.box.scheduler.BoxClearScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class BoxClearLogMain {
    public static void main(String[] args) throws Exception{
        /**
         * 设置用户名
         * 1.设置环境变量
         * 2.设置系统变量  System.setProperty("HADOOP_USER_NAME");
         * 获取用户名:
         *   1.System.getenv("HADOOP_USER_NAME");
         *   2.为空时，System.getProperty("HADOOP_USER_NAME");
         */

        if (args.length < 3) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new BoxClearScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
