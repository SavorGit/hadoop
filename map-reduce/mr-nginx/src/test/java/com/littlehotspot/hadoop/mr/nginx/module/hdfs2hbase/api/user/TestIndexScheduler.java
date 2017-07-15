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
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.boxindex.BoxUserIndex;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.AllUser;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.NginxUser;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.UserFinal;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.UserProjectLog;
import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.RqUser;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
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
public class TestIndexScheduler {


    @Test
    public void run() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsIn=/home/data/hadoop/flume/test-mr/box_user",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-nginx_user",
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
            ToolRunner.run(conf, new BoxUserIndex(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
