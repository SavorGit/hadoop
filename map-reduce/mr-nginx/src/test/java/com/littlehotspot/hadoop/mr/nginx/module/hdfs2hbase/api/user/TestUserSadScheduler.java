package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.SadActType;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.SadType;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.SadActScheduler;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.UserSadScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

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
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection_start"
        };
        runStartOrEnd(args, SadActType.START_PRO);
    }

    @Test
    public void testEnd_Pro() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/source/box_log_distinct",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection_end"
        };
        runStartOrEnd(args, SadActType.END_PRO);
    }

    @Test
    public void testStart_Dem() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/source/box_log_distinct",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_demand_start"
        };
        runStartOrEnd(args, SadActType.START_DEM);
    }

    @Test
    public void testEnd_Dem() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test_hbase/source/box_log_distinct",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_demand_end"
        };
        runStartOrEnd(args, SadActType.END_DEM);
    }

    private void runStartOrEnd(String[] args, SadActType sadType){

        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        conf.set("sadType", sadType.name());
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
                "hdfsIn=/home/data/hadoop/flume/test_hbase/user_projection_start;/home/data/hadoop/flume/test_hbase/user_projection_end",
                "hdfsOut=/home/data/hadoop/flume/test_hbase/user_projection"
        };
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        conf.set("sadType", SadType.PROJECTION.name());
        try {
            ToolRunner.run(conf, new UserSadScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void te() throws IOException {
        long d1 = new Date().getTime();

        System.out.println(new Date().getTime()-d1);
    }

    @Test
    public void testclass(){
        System.out.println(Long.MAX_VALUE);
    }
}
