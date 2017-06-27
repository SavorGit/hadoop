/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:43
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Matcher;

/**
 * <h1>模型 - [源] 用户</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月02日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@Data
@NoArgsConstructor
public class TextSourceUserBean {

    /**
     * HTTP Client-IP
     */
    private String clientIP;

    /**
     * Access-Timestamp
     */
    private String timestamp;

    /**
     * HTTP-Request-Method
     */
    private String requestMethod;

    /**
     * HTTP Request URI
     */
    private String requestURI;

    /**
     * HTTP-Response-Status
     */
    private String responseStatus;

    /**
     * HTTP-Header[referer]
     */
    private String refererHeader;

    /**
     * HTTP-Header[user_agent]
     */
    private String userAgentHeader;

    /**
     * HTTP-Header[x_forwarded_for]
     */
    private String xForwardedForHeader;

    /**
     * Access-Time
     */
    private String accessTime;

    /**
     * 版本名称
     */
    private String versionName;

    /**
     * 版本号
     */
    private String versionCode;

    /**
     * 手机系统版本
     */
    private String buildVersion;

    /**
     * 系统 API 版本
     */
    private String osVersion;

    /**
     * 机器型号
     */
    private String machineModel;

    /**
     * 应用名称
     */
    private String appName;

    /**
     * 设备 ID
     */
    private String deviceId;

    /**
     * 设备类型
     */
    private String deviceType;

    /**
     * 渠道 ID
     */
    private String channelId;

    /**
     * 渠道名称
     */
    private String channelName;

    /**
     * 网络类型
     */
    private String network;

    /**
     * 语言
     */
    private String language;

    public TextSourceUserBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_NGINX_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setClientIP(matcher.group(1));
        this.setTimestamp(matcher.group(2));
        this.setRequestMethod(matcher.group(3));
        this.setRequestURI(matcher.group(4));
        this.setResponseStatus(matcher.group(5));
        this.setRefererHeader(matcher.group(6));
        this.setVersionName(matcher.group(7));
        this.setVersionCode(matcher.group(8));
        this.setBuildVersion(matcher.group(9));
        this.setOsVersion(matcher.group(10));
        this.setMachineModel(matcher.group(11));
        this.setAppName(matcher.group(12));
        this.setDeviceType(matcher.group(13));
        this.setChannelId(matcher.group(14));
        this.setChannelName(matcher.group(15));
        this.setDeviceId(matcher.group(16));
        this.setNetwork(matcher.group(17));
        this.setLanguage(matcher.group(18));
        this.setUserAgentHeader(matcher.group(19));
        this.setXForwardedForHeader(matcher.group(20));
        this.setAccessTime(this.cleanValue(matcher.group(21)));
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
