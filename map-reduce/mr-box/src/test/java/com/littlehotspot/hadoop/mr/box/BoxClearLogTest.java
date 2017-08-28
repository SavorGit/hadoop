package com.littlehotspot.hadoop.mr.box;

import com.littlehotspot.hadoop.mr.box.scheduler.BoxClearScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class BoxClearLogTest {

    @Test
    public void run() {
        /**
         * 设置用户名
         * 1.设置环境变量
         * 2.设置系统变量  System.setProperty("HADOOP_USER_NAME");
         * 获取用户名:
         *   1.System.getenv("HADOOP_USER_NAME");
         *   2.为空时，System.getProperty("HADOOP_USER_NAME");
         */
       try{
            Configuration conf = new Configuration();
            String[] args = new String[3];
            args[0]="hdfsCluster=hdfs://devpd1:8020";
            args[1]="hdfsIn=/home/data/hadoop/flume/box_export/2017-07-20";
            args[2]="hdfsOut=/home/data/hadoop/flume/tmp/clear/box_export/2017-07";
            ToolRunner.run(conf, new BoxClearScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
