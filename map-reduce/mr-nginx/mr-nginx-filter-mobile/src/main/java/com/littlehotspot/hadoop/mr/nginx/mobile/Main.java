/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:52
 */
package com.littlehotspot.hadoop.mr.nginx.mobile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月22日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class Main {

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
            ToolRunner.run(conf, new NginxLog(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
