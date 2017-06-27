/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:29
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * <h1>Mapper - 用户</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class UserReadMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
//            System.out.println(rowLineContent);
            Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }
            String deviceId = matcher.group(10);
            if (StringUtils.isBlank(deviceId)) {
                return;
            }
            Text keyText = new Text(deviceId);
            System.out.println(rowLineContent);
            context.write(keyText, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
