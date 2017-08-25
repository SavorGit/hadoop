package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.probelog.ProbeToHbase;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.probelog.ProbeToHdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-25 上午 10:43.
 */
public class TestProbe {
    @Test
    public void test1(){
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/probe_source",
                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/probe_export/test",

//                "hbaseRoot=hdfs://localhost:9000/hbase",
//                "hbaseZookeeper=localhost",

//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
        };
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ProbeToHdfs(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test2(){
        String[] args = {
//                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/probe_export/test",
                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/probe_export/test1",

//                "hbaseRoot=hdfs://devpd1:8020/hbase",
//                "hbaseZookeeper=devpd1",
//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",

        };
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ProbeToHbase(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws ParseException {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm");
        String time = "2017-08-22 14:15:34";
        System.out.println(format.parse(time).getTime());

    }
}
