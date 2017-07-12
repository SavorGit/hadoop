package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias.MediaScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources.ResourceScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-10 下午 7:52.
 */
public class TestMedia {
    @Test
    public void test(){
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/mysql",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/mysql1",

                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new MediaScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
