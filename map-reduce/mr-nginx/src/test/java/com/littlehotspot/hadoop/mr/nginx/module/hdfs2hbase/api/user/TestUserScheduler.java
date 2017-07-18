/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:34
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.*;
import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.RqUser;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.Tags;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.util.List;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestUserScheduler {


    @Test
    public void run() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsIn=/home/data/hadoop/flume/test-mr/nginx_user",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-nginx_user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new NginxUser(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void userDema() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsIn=/home/data/hadoop/flume/test-mr/box_user",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-user_pro",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new UserProjectLog(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void allUser() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsBoxIn=/home/data/hadoop/flume/test-mr/test-mob_user1",
                "hdfsMobIn=/home/data/hadoop/flume/test-mr/test-box_user1",
                "hdfsNgxIn=/home/data/hadoop/flume/test-mr/test-nginx_user1",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new AllUser(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void user() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsUserIn=/home/data/hadoop/flume/test-mr/test-all_user",
                "hdfsProIn=/home/data/hadoop/flume/test-mr/test-user_demand1",
                "hdfsDemaIn=/home/data/hadoop/flume/test-mr/test-user_pro1",
                "hdfsReadIn=/home/data/hadoop/flume/test-mr/test-user_read1",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-user_final",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1",
                "hdfsCluster=hdfs://devpd1:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new UserFinal(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void rqUser() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
////                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
//            ToolRunner.run(conf, new MobileUser(), args);
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(RqUser.class);
            selectModel.setQuery("select source_type,clientid,deviceid,UNIX_TIMESTAMP(min(add_time)) from savor_download_count where deviceid is not null group by deviceid");
            selectModel.setCountQuery("select count(*) from savor_download_count");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_mr/test_rquser");
            JdbcReader.readRquserToHdfs("hdfs://devpd1:8020",selectModel);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAll() {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            List<Result> user = new HBaseHelper(conf).getAllRecord("user");

            for (Result result : user) {
                System.out.println(result.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
