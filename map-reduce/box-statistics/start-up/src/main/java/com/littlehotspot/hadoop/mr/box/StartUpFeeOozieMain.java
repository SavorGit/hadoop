/**
 * Copyright (c) 2020, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 11:47
 */
package com.littlehotspot.hadoop.mr.box;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.oozie.action.hadoop.LauncherMain;

/**
 * Oozie调用主方法类
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2020年10月20日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class StartUpFeeOozieMain {

    /**
     * 主方法。
     *
     * @param args 参数列表。参数名：
     *             usage: Options
     *             -h,--help             Print options' information
     *             -i,--input <args>     The paths of data input
     *             -jn,--jobName <arg>   The name of job
     *             -o,--output <arg>     The path of data output
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = LauncherMain.loadActionConf();
        ToolRunner.run(configuration, new StartUpFee(), args);
    }
}
