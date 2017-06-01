/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.cdf
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:02
 */
package com.littlehotspot.hadoop.mr.nginx.module.cdf;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.util.ArgumentUtil;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * <h1>Mapper - 数据格式转换</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class CDFMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
            Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(rowLineContent);
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

        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.VersionName.getName(), Argument.VersionName.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 版本名称
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.VersionCode.getName(), Argument.VersionCode.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 版本号
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.BuildVersion.getName(), Argument.BuildVersion.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 手机系统版本
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.OSVersion.getName(), Argument.OSVersion.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 系统 API 版本
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.MachineModel.getName(), Argument.MachineModel.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 机器型号
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.AppName.getName(), Argument.AppName.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 应用名称
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.DeviceType.getName(), Argument.DeviceType.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 设备类型
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.ChannelId.getName(), Argument.ChannelId.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 渠道 ID
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.ChannelName.getName(), Argument.ChannelName.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 渠道名称
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.DeviceId.getName(), Argument.DeviceId.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 设备 ID
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.Network.getName(), Argument.Network.getDefaultValue())).append(Constant.VALUE_SPLIT_CHAR);// 网络类型
        traceInfoStringBuffer.append(ArgumentUtil.getParameterValue(parameterMap, Argument.Language.getName(), Argument.Language.getDefaultValue()));// 语言

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
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constant.DATA_FORMAT_1, Locale.US);
        Date date = simpleDateFormat.parse(dateString);
        return date.getTime();
    }

    // NGINX 日志时间格式转换
    private String turnDateFormat(String srcDateString) throws ParseException {
        String dateString = this.turnDataForNone(srcDateString);
        if (StringUtils.isBlank(dateString)) {
            return "";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constant.DATA_FORMAT_1, Locale.US);
        Date date = simpleDateFormat.parse(dateString);
        String tarDateString = DateFormatUtils.format(date, Constant.DATA_FORMAT_2);
        return tarDateString;
    }
}
