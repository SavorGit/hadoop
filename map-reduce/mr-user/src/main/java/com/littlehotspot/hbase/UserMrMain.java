package com.littlehotspot.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 *@Author 刘飞飞
 *@Date 2017/7/19 17:19
 */
public class UserMrMain {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new UserMr(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
