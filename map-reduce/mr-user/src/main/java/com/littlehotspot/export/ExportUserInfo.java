package com.littlehotspot.export;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 *@Author 刘飞飞
 *@Date 2017/7/18 16:18
 */
public class ExportUserInfo {
    public static void main(String[] args) throws IOException {
        if(args.length<1){
            throw new IOException("please write excel export path");
        }
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new ExportTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
