/**
 * Copyright (c) 2020, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.netty.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 11:02
 */
package com.littlehotspot.hadoop.mr.netty.box;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2020年12月25日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@SuppressWarnings("JavaDoc")
public class TestConvertConnectionData {

    @Before
    public void initHadoopEnv() {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        System.setProperty("mapreduce.job.outputformat.class", TextOutputFormat.class.toString());
        System.setProperty("HADOOP_USER_NAME", "lizhao");
    }

    /**
     * usage: Options
     * -h,--help             Print options' information
     * -i,--input <args>     The paths of data input
     * -jn,--jobName <arg>   The name of job
     * -o,--output <arg>     The path of data output
     */
    @Test
    public void testHelp() {
        try {
            String[] args = {"-h"};
            ConvertConnectionData.Main.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * usage: Options
     * -h,--help             Print options' information
     * -i,--input <args>     The paths of data input
     * -jn,--jobName <arg>   The name of job
     * -o,--output <arg>     The path of data output
     */
    @Test
    public void testDefault() {
        try {
            String[] args = {"-jn=test job", "-i", "E:\\hadoop-data\\source", "-o=E:\\hadoop-data\\export\\" + System.currentTimeMillis()};
            ConvertConnectionData.Main.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * usage: Options
     * -h,--help             Print options' information
     * -i,--input <args>     The paths of data input
     * -jn,--jobName <arg>   The name of job
     * -o,--output <arg>     The path of data output
     */
    @Test
    public void testDefaultLocal() {
        try {
            String[] args = {"-jn=test job", "--verbose", "-i", "E:\\hadoop-data\\source", "-o=E:\\hadoop-data\\export\\" + System.currentTimeMillis()};
            Configuration configuration = new Configuration();
            configuration.set("mapreduce.framework.name", "local");//以local形式提交
            ToolRunner.run(configuration, new ConvertConnectionData(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * usage: Options
     * -h,--help             Print options' information
     * -i,--input <args>     The paths of data input
     * -jn,--jobName <arg>   The name of job
     * -o,--output <arg>     The path of data output
     */
    @Test
    public void testLocal() {// 无效
        try {
            String[] args = {"-jn=test job", "-i", "hdfs://onlined1:8020/home/data/flume/netty/conn/source/2020-09-17,hdfs://onlined1:8020/home/data/flume/netty/conn/source/2020-09-18", "-o=/test/lizhao"};
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://localhost:9000");
            configuration.set("mapreduce.framework.name", "local");//以local形式提交
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000/"), configuration);
            fileSystem.delete(new Path("/test/lizhao"));
            ToolRunner.run(configuration, new ConvertConnectionData(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * usage: Options
     * -h,--help             Print options' information
     * -i,--input <args>     The paths of data input
     * -jn,--jobName <arg>   The name of job
     * -o,--output <arg>     The path of data output
     */
    @Test
    public void testYarn() {
        try {
            String[] args = {"-jn=test job", "-i", "/home/data/flume/netty/conn/source/2020-09-17,/home/data/flume/netty/conn/source/2020-09-18", "-o=/test/lizhao"};
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://onlined1:8020");
            configuration.set("mapreduce.job.jar", "E:\\WorkSpace\\Company\\Savor\\Git\\JAVA\\hadoop\\map-reduce\\box-statistics\\start-up\\target\\start-up-LHS.HADOOP.2.11.1.0.1.0.0-SNAPSHOT.jar");//指定Jar包，也可以在job中设置
            configuration.set("mapreduce.framework.name", "yarn");//以yarn形式提交
            configuration.set("yarn.resourcemanager.hostname", "onlined1");
            configuration.set("mapreduce.app-submission.cross-platform", "true");//跨平台提交
            ToolRunner.run(configuration, new ConvertConnectionData(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
