package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-07 下午 5:14.
 */
public class HBaseToMysqlMain {
    public static void main(String[] args) throws IOException {

        args = new String[]{
                "jdbcDriver=com.mysql.jdbc.Driver",
                "jdbcUrl=jdbc:mysql://192.168.2.145:3306/cloud",
                "jdbcUsername=javaweb",
                "jdbcPassword=123456",
        };
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");

        if(args.length < 4){
            throw new IOException("please write mysql connection...");
        }
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new HbaseToMysqlScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
