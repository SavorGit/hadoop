package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources.ResourceScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-06 下午 4:07.
 */
public class TestResources {
    @Test
    public void test(){
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/mysql",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/mysql1",

                "resourceType=CON",

                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ResourceScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
