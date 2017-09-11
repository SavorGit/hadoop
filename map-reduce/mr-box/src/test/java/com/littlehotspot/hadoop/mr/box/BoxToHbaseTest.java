package com.littlehotspot.hadoop.mr.box;

import com.littlehotspot.hadoop.mr.box.hbase.scheduler.BoxToHFileScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class BoxToHbaseTest {

    @Test
    public void run() {
        try {
            Configuration conf = new Configuration();
            String[] args = new String[6];
            args[0]="hdfsCluster=hdfs://devpd1:8020";
            args[1]="hdfsIn=/home/data/hadoop/flume/tmp/integ/box_export/2017-07";
            args[2]="hdfsOut=/home/data/hadoop/flume/tmp/hbase/box_export/2017-07";
            args[3]="hbaseRoot=hdfs://devpd1:8020/hbase";
            args[4]="hbaseZookeeper=devpd1";
            args[5]="hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase";
//            ToolRunner.run(conf, new BoxToHbaseScheduler(), args);
            ToolRunner.run(conf, new BoxToHFileScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
