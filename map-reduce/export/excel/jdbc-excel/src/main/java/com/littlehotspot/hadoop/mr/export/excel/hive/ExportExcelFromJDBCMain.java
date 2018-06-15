/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.hive
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:29
 */
package com.littlehotspot.hadoop.mr.export.excel.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.oozie.action.hadoop.LauncherMain;

/**
 * <h1>主类 - 通过 JDBC 导出 Excel</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月15日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ExportExcelFromJDBCMain {

    /**
     * 主方法。
     *
     * @param args 参数列表。参数名：
     *             jobName      任务名称(可选)
     *             hdfsCluster  Hdfs 集群地址(可选)
     *             inRegex      输入 Mapper 的正则表达式
     *             hdfsIn       输入的 HDFS 路径
     *             hdfsOut      输出的 HDFS 路径
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("please write input path and output path...");
        }
        Configuration configuration = LauncherMain.loadActionConf();
        ToolRunner.run(configuration, new ExportExcelFromJDBCScheduler(), args);
    }
}
