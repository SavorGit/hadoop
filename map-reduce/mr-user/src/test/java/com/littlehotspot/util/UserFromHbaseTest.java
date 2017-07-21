package com.littlehotspot.util;

import com.littlehotspot.hbase.UserMr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class UserFromHbaseTest {

    @Test
    public void run() {
        try {
            Configuration conf = new Configuration();
            String[] args = new String[6];
            args[0]="hdfsCluster=hdfs://devpd1:8020";
            args[1]="excelOutPath=/excel.xlsx";
            args[2]="hdfsOut=/home/data/hadoop/flume/tmp/hbase/user_hbase";
            args[3]="hbaseRoot=hdfs://devpd1:8020/hbase";
            args[4]="hbaseZookeeper=devpd1";
            args[5]="hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase";
            ToolRunner.run(conf, new UserMr(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
