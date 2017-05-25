/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 11:03
 */
package com.littlehotspot.hadoop.mr.nginx.mobile;

import com.littlehotspot.hadoop.mr.nginx.mobile.util.ArgumentUtil;
import com.littlehotspot.hadoop.mr.nginx.mobile.util.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * <h1>Mapper - Nginx</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月24日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class NginxMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
            Matcher matcher = NginxLog.MAPPER_INPUT_FORMAT_REGEX.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }
//                if (StringUtils.isBlank(matcher.group(7)) || "-".equalsIgnoreCase(matcher.group(7).trim())) {
//                    return;
//                }

            StringBuffer newValueStringBuffer = new StringBuffer();
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(1))).append(Constant.VALUE_SPLIT_CHAR);// Client-IP
            newValueStringBuffer.append(this.toTimestamp(matcher.group(2))).append(Constant.VALUE_SPLIT_CHAR);// Access-Timestamp
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(3))).append(Constant.VALUE_SPLIT_CHAR);// HTTP-Request-Method
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(4))).append(Constant.VALUE_SPLIT_CHAR);// URI
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(5))).append(Constant.VALUE_SPLIT_CHAR);// HTTP-Response-Status
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(6))).append(Constant.VALUE_SPLIT_CHAR);// HTTP-Header[referer]
            newValueStringBuffer.append(this.analysisTraceInfo(matcher.group(7))).append(Constant.VALUE_SPLIT_CHAR);// HTTP-Header[traceinfo]
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(8))).append(Constant.VALUE_SPLIT_CHAR);// HTTP-Header[user_agent]
            newValueStringBuffer.append(this.turnDataForNone(matcher.group(9))).append(Constant.VALUE_SPLIT_CHAR);// HTTP-Header[x_forwarded_for]
            newValueStringBuffer.append(this.turnDateFormat(matcher.group(2)));// Access-Time

            context.write(new Text(newValueStringBuffer.toString()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String analysisTraceInfo(String traceInfo) {
        StringBuffer traceInfoStringBuffer = new StringBuffer();
        if (StringUtils.isBlank(traceInfo)) {
            return traceInfoStringBuffer.toString();
        }
        String[] parameterArray = traceInfo.trim().split(";");
        Map<String, List<String>> parameterMap = ArgumentUtil.analysisArgument(parameterArray);

        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.VersionName.getName(), ArgumentTraceInfo.VersionName.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 版本名称
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.VersionCode.getName(), ArgumentTraceInfo.VersionCode.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 版本号
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.BuildVersion.getName(), ArgumentTraceInfo.BuildVersion.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 手机系统版本
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.OSVersion.getName(), ArgumentTraceInfo.OSVersion.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 系统 API 版本
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.MachineModel.getName(), ArgumentTraceInfo.MachineModel.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 机器型号
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.AppName.getName(), ArgumentTraceInfo.AppName.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 应用名称
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.DeviceType.getName(), ArgumentTraceInfo.DeviceType.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 设备类型
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.ChannelId.getName(), ArgumentTraceInfo.ChannelId.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 渠道 ID
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.ChannelName.getName(), ArgumentTraceInfo.ChannelName.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 渠道名称
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.DeviceId.getName(), ArgumentTraceInfo.DeviceId.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 设备 ID
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.Network.getName(), ArgumentTraceInfo.Network.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 网络类型
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, ArgumentTraceInfo.Language.getName(), ArgumentTraceInfo.Language.getDefaultValue()));// 语言

        return traceInfoStringBuffer.toString();
    }

    // NGINX 日志空数据转换
    private String turnDataForNone(String data) {
        if (StringUtils.isBlank(data)) {
            return "";
        }
        if ("-".equalsIgnoreCase(data)) {
            return "";
        }
        return data;
    }

    // NGINX 日志日期转时间截
    private long toTimestamp(String srcDateString) throws ParseException {
        String dateString = this.turnDataForNone(srcDateString);
        if (StringUtils.isBlank(dateString)) {
            return 0;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constant.DATA_FORMAT_SRC, Locale.US);
        Date date = simpleDateFormat.parse(dateString);
        return date.getTime();
    }

    // NGINX 日志时间格式转换
    private String turnDateFormat(String srcDateString) throws ParseException {
        String dateString = this.turnDataForNone(srcDateString);
        if (StringUtils.isBlank(dateString)) {
            return "";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constant.DATA_FORMAT_SRC, Locale.US);
        Date date = simpleDateFormat.parse(dateString);
        String tarDateString = DateFormatUtils.format(date, Constant.DATA_FORMAT_TAR);
        return tarDateString;
    }

//    // 获取 NGINX 日志自定义参数值
//    private String getCustomParameterValue(Map<String, String> map, String key, String defaultValue) {
//        if (map == null) {
//            throw new IllegalArgumentException("The map is null");
//        }
//        String value = map.get(key);
//        return value == null ? defaultValue : value;
//    }
}
