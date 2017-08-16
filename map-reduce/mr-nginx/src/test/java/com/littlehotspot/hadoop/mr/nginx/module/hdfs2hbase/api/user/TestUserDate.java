package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.userDate.UserDateScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-14 下午 3:34.
 */
public class TestUserDate {
    @Test
    public void test(){
        String[] args=new String[]{
                "actType=DEMAND",
                "date=20170612",
                "hBaseTableSource=user_demand",
                "hBaseTable=user_date",
                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/TEST",
        };
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        try {
            ToolRunner.run(conf, new UserDateScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void test1(){
        String[] args=new String[]{
                "actType=PROJECTION",
                "date=20170612",
                "hBaseTableSource=user_projection",
                "hBaseTable=user_date",
                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/TEST",
        };
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        try {
            ToolRunner.run(conf, new UserDateScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void test2(){
        String[] args=new String[]{
                "actType=READ",
                "date=20170612",
                "hBaseTableSource=user_read",
                "hBaseTable=user_date",
                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/TEST",
        };
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        try {
            ToolRunner.run(conf, new UserDateScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
