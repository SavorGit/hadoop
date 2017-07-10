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
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.tags.TagsLog;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.MobileUser;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.UserDemaLog;
import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.Category;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.TagList;
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
public class TestTagsScheduler {


    @Test
    public void run() {
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
            selectModel.setInputClass(Tags.class);
            selectModel.setQuery("select article_id,tagid,tagname from savor_tag");
            selectModel.setCountQuery("select count(*) from savor_tag");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_mr/test_tag");
            JdbcReader.readTagToHdfs("hdfs://devpd1:8020",selectModel);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tagList() {
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
            selectModel.setInputClass(TagList.class);
            selectModel.setQuery("select id,tagname from savor_taglist");
            selectModel.setCountQuery("select count(*) from savor_taglist");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_mr/test_list");
            JdbcReader.readTagListToHdfs("hdfs://devpd1:8020",selectModel);

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
                "hdfsIn=/home/data/hadoop/flume/box_export/2017-07-01",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-user_pro1",
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
            ToolRunner.run(conf, new UserDemaLog(), args);
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
                "hdfsUserIn=/home/data/hadoop/flume/test-mr/mob_user",
                "hdfsProIn=/home/data/hadoop/flume/test-mr/mob_user",
                "hdfsDemaIn=/home/data/hadoop/flume/test-mr/mob_user",
                "hdfsReadIn=/home/data/hadoop/flume/test-mr/mob_user",
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
            ToolRunner.run(conf, new MobileUser(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insert() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/nginx_log/export/test-hbase",
                "hdfsInStart=/home/data/hadoop/flume/test_mr/test_list",
                "hdfsInEnd=/home/data/hadoop/flume/test_mr/test_tag",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-tag",
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
            ToolRunner.run(conf, new TagsLog(), args);
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
