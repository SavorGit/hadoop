package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.SadActScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.UserSadScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-26 下午 3:49.
 */
public class TestUserSadScheduler {
    @Test
    public void testStart_Pro() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/source/box_log_distinct",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection_start",
                "sadActType=START_PRO"
        };
        runStartOrEnd(args);
    }

    @Test
    public void testEnd_Pro() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/source/box_log_distinct",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection_end",
                "sadActType=END_PRO"
        };
        runStartOrEnd(args);
    }

       @Test
    public void testStart_Dem() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/box_export/2017-07-03",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_demand_start",
                "sadActType=START_DEM"
        };
        runStartOrEnd(args);
    }

    @Test
    public void testEnd_Dem() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/box_export/2017-07-03",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_demand_end",
                "sadActType=END_DEM"
        };
        runStartOrEnd(args);
    }

    private void runStartOrEnd(String[] args){

        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new SadActScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void user_project() throws IOException {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsInStart=/home/data/hadoop/flume/test_hbase/user_projection_start",
                "hdfsInEnd=/home/data/hadoop/flume/test_hbase/user_projection_end",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection",
                "sadType=PROJECTION",

                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new UserSadScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void user_demand() throws IOException {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsInStart=/home/data/hadoop/flume/test_hbase/user_demand_start",
                "hdfsInEnd=/home/data/hadoop/flume/test_hbase/user_demand_end",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection",
                "sadType=DEMAND",

                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new UserSadScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
