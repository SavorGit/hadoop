package com.littlehotspot.hadoop.mr.box;

import com.littlehotspot.hadoop.mr.box.scheduler.BoxIntegratedScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class BoxJdbcDataTest {

    @Test
    public void run() {
        try {
            Configuration conf = new Configuration();
            String[] args = new String[3];
            args[0]="hdfsCluster=hdfs://devpd1:8020";
            args[1]="hdfsIn=/home/data/hadoop/flume/tmp/clear/box_export/2017-07";
            args[2]="hdfsOut=/home/data/hadoop/flume/tmp/integ/box_export/2017-07";
            ToolRunner.run(conf, new BoxIntegratedScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
