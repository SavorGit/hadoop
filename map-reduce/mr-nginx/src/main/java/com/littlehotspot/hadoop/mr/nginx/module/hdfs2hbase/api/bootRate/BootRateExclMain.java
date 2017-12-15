///**
// * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
// * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
// *
// * @Project : hadoop
// * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
// * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
// * @EMAIL 404644381@qq.com
// * @Time : 15:33
// */
//package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.util.GenericOptionsParser;
//
///**
// * <h1>主类 - 用户 [API]</h1>
// *
// * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
// * @version 1.0.0.0.1
// * @notes Created on 2017年06月01日<br>
// * Revision of last commit:$Revision$<br>
// * Author of last commit:$Author$<br>
// * Date of last commit:$Date$<br>
// */
//public class BootRateExclMain {
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        try {
//            BootRateExcl bootRateExcl = new BootRateExcl(conf);
//            GenericOptionsParser parser = new GenericOptionsParser(conf, args);
//            String[] toolArgs = parser.getRemainingArgs();
//            bootRateExcl.run(toolArgs);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
