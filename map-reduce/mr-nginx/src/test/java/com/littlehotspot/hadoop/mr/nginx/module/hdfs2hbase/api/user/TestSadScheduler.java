package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.all_data.SadAllDataScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.all_day.SadAllDayScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.month.SadMonthScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.year.SadYearScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.yesterday.SadYesterdayScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 投屏点播测试类
 */
public class TestSadScheduler {


    @Test
    public void runYesterday() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/user/hive/warehouse/sad_yesterday_export",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-hbase/sad_yesterday_export"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new SadYesterdayScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void runMonth() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/user/hive/warehouse/sad_thismonth_export",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-hbase/sad_thismonth_export"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new SadMonthScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void runYear() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/user/hive/warehouse/sad_thisyear_export",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-hbase/sad_thisyear_export"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        conf.set("hbase.rootdir","hdfs://devpd1:8020/hbase");
        conf.set("hbase.zookeeper.quorum","devpd1");

        try {
            ToolRunner.run(conf, new SadYearScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void runAllDay() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsIn=/user/hive/warehouse/sad_allday_export",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-hbase/sad_allday_export"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new SadAllDayScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void runAllData() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/user/hive/warehouse/sad_alldata_export",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-hbase/sad_alldata_export"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new SadAllDataScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clean(){
        clean("sad_yesterday_export");
        clean("sad_thismonth_export");
        clean("sad_thisyear_export");
        clean("sad_allday_export");
        clean("sad_alldata_export");
    }

    @Test
    public void getAll(){
        getAll("sad_alldata_export");
    }

    public void clean(String tableName){
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            HBaseHelper hBaseHelper = new HBaseHelper(conf);
            hBaseHelper.deleteTable(tableName);
            hBaseHelper.createTable(tableName,new String[]{"basic","info"});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getAll(String tableName){
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            List<Result> list = new HBaseHelper(conf).getAllRecord(tableName);
//            for (Result result : list) {
//                System.out.println(result.toString());
//            }
            System.out.println(list.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
