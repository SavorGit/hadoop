/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:36
 */
package com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase;

import com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.hfile.HFileScheduler;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * <h1>Mapper -  用户</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月26日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class UserMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    //    ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
            Matcher matcher = HFileScheduler.MAPPER_INPUT_FORMAT_REGEX.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }

            String clientIp = matcher.group(1);// Client-IP
            String accessTimestamp = matcher.group(2);// Access-Timestamp
            String httpRequestMethod = matcher.group(3);// HTTP-Request-Method
            String requestURI = matcher.group(4);// URI
            String httpResponseStatus = matcher.group(5);// HTTP-Response-Status
            String httpHeaderReferer = matcher.group(6);// HTTP-Header[referer]
            String versionName = matcher.group(7);// 版本名称
            String versionCode = matcher.group(8);// 版本号
            String buildVersion = matcher.group(9);// 手机系统版本
            String osVersion = matcher.group(10);// 系统 API 版本
            String machineModel = matcher.group(11);// 机器型号
            String appName = matcher.group(12);// 应用名称
            String deviceType = matcher.group(13);// 设备类型
            String channelId = matcher.group(14);// 渠道 ID
            String channelName = matcher.group(15);// 渠道名称
            String deviceId = matcher.group(16);// 设备 ID
            String network = matcher.group(17);// 网络类型
            String language = matcher.group(18);// 语言
            String httpHeaderUserAgent = matcher.group(19);// HTTP-Header[user_agent]
            String httpHeaderXForwardedFor = matcher.group(20);// HTTP-Header[x_forwarded_for]
            String datetimeGMT = matcher.group(21);// Access-Time

            if (StringUtils.isBlank(deviceId)) {
                return;
            }
            byte[] deviceIdBytes = Bytes.toBytes(deviceId);
            ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(deviceIdBytes);

            Put put = new Put(deviceIdBytes);

            put.addColumn(Bytes.toBytes("article"), Bytes.toBytes("deviceType"), Bytes.toBytes(deviceType));
            put.addColumn(Bytes.toBytes("article"), Bytes.toBytes("machineModel"), Bytes.toBytes(machineModel));

            context.write(rowKeyWritable, put);

//            rowKey.set(deviceIdBytes);
//            context.write(rowKey, put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
