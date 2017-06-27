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
public class SourceUserBean {

    /**
     * HTTP Client-IP
     */
    private String mobileId;

    /**
     * Access-Timestamp
     */
    private String deviceType;

    /**
     * HTTP-Request-Method
     */
    private String deviceModel;

    /**
     * HTTP Request URI
     */
    private String deviceToken;

    /**
     * HTTP-Response-Status
     */
    private String downloadTimestamps;

    /**
     * HTTP-Header[referer]
     */
    private String downloadTime;

    /**
     * HTTP-Header[user_agent]
     */
    private String downloadHotel;

    /**
     * HTTP-Header[x_forwarded_for]
     */
    private String downloadSrc;

    /**
     * Access-Time
     */
    private String demandTimestamps;

    /**
     * 版本名称
     */
    private String demandTime;

    /**
     * 版本号
     */
    private String demandHotel;

    /**
     * 手机系统版本
     */
    private String projectionTimestamps;

    /**
     * 系统 API 版本
     */
    private String projectionTime;

    /**
     * 机器型号
     */
    private String projectionHotel;



    public SourceUserBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_NGINX_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setMobileId(matcher.group(1));
        this.setDeviceType(matcher.group(2));
        this.setDeviceModel(matcher.group(3));
        this.setDeviceToken(matcher.group(4));
        this.setDownloadTimestamps(matcher.group(5));
        this.setDownloadTime(matcher.group(6));
        this.setDownloadHotel(matcher.group(7));
        this.setDownloadSrc(matcher.group(8));
        this.setDemandTimestamps(matcher.group(9));
        this.setDemandTime(matcher.group(10));
        this.setDemandHotel(matcher.group(11));
        this.setProjectionTimestamps(matcher.group(12));
        this.setProjectionTime(matcher.group(13));
        this.setProjectionHotel(matcher.group(14));
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
