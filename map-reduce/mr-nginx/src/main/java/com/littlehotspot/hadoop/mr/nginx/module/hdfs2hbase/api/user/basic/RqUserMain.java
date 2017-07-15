/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:33
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic;

import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.RqUser;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * <h1>主类 - 用户 [API]</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class RqUserMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();
//
//        // 配置 HDFS 根路径
//        if (args.length > 3) {
//            conf.set("fs.defaultFS", args[3]);
////            conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        }

        try {
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
}
