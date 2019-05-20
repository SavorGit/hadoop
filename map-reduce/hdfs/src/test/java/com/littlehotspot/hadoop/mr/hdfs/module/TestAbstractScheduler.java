/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.module
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:35
 */
package com.littlehotspot.hadoop.mr.hdfs.module;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;

/**
 * <h1>单元测试 - 抽象</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2019年05月17日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public abstract class TestAbstractScheduler {
    protected static final String FORMAT_PRINT_TIME_CONSUMING = "执行 %s 用时 %s 毫秒";
    protected static final String FORMAT_COLOR_PRINT_TIME_CONSUMING = "@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@\n";

    protected DecimalFormat decimalFormat;

    protected Configuration conf;

    @Before
    public void init() throws IOException {
        System.out.println();
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        Locale.setDefault(Locale.CHINA);
        this.decimalFormat = new DecimalFormat();

        File localCachePath = this.mkdir("D:/hadoop/cache");

        this.conf = new Configuration();
        //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
        this.conf.set("mapreduce.framework.name", "local");
        //本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
        //到底在哪里，就看以下两行配置你用哪行，默认就是file:///
        conf.set("fs.defaultFS", "file:///");

//        this.conf.set("mapreduce.cluster.local.dir", localCachePath.getCanonicalPath());//在此处有一坑，本地需要添加一个缓存文件夹
//        this.conf.set("fs.defaultFS", "hdfs://onlined1:8020");
//        this.conf.set("yarn.resourcemanager.hostname", "onlined1");
//        this.conf.set("yarn.resourcemanager.address", "8032");
//        this.conf.set("mapreduce.framework.name", "yarn");
//        this.conf.set("mapreduce.app-submission.cross-platform", "true");
//        conf.set("mapred.jar", "mapreduce/build/libs/mapreduce-0.1.jar"); // 也可以在这里设置刚刚编译好的jar
//        conf.set("mapred.job.tracker", "onlined1:9001");

//        this.conf.setBoolean("dfs.permissions", false);
//        this.conf.set("mapred.job.tracker", "local");
//        this.conf.set("mapreduce.framework.name", "yarn");
//        this.conf.set("yarn.resoucemanager.hostname", "localhost");
//        this.conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
//        this.conf.set("yarn.resourcemanager.address", "localhost:8032");
//        this.conf.set("yarn.resourcemanager.resource-tracker.address", "localhost:8035");
//        this.conf.set("yarn.resourcemanager.admin.address", "localhost:8033");
    }

    private File mkdir(String directory) throws IOException {
        return this.mkdir(new File(directory));
    }

    private File mkdir(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                String message = "File " + directory + " exists and is not a directory. Unable to create directory.";
                throw new IOException(message);
            }
        } else {
            if (!directory.mkdirs()) {
                // Double-check that some other thread or process hasn't made
                // the directory in the background
                if (!directory.isDirectory()) {
                    String message = "Unable to create directory " + directory;
                    throw new IOException(message);
                }
            }
        }
        return directory;
    }
}
