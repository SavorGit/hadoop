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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read;

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
public class SourceUserReadBean {

    /**
     * HTTP Client-IP
     */
    private String mobileId;

    /**
     * Access-Timestamp
     */
    private String startTime;

    /**
     * HTTP-Request-Method
     */
    private String endTime;

    /**
     * HTTP Request URI
     */
    private String duration;

    /**
     * HTTP-Response-Status
     */
    private String contentId;

    /**
     * HTTP-Header[referer]
     */
    private String title;

    /**
     * HTTP-Header[user_agent]
     */
    private String content;

    /**
     * HTTP-Header[x_forwarded_for]
     */
    private String longitude;

    /**
     * Access-Time
     */
    private String latitude;

    /**
     * 版本名称
     */
    private String osType;

    /**
     * 版本号
     */
    private String categoryId;

    /**
     * 手机系统版本
     */
    private String categoryName;

    /**
     * 系统 API 版本
     */
    private String hotelId;

    /**
     * 机器型号
     */
    private String hotelName;

    /**
     * 机器型号
     */
    private String roomId;
    /**
     * 机器型号
     */
    private String roomName;




    public SourceUserReadBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setMobileId(matcher.group(1));
        this.setStartTime(matcher.group(2));
        this.setEndTime(matcher.group(3));
        this.setDuration(matcher.group(4));
        this.setContentId(matcher.group(5));
        this.setTitle(matcher.group(6));
        this.setContent(matcher.group(7));
        this.setLongitude(matcher.group(8));
        this.setLatitude(matcher.group(9));
        this.setOsType(matcher.group(10));
        this.setCategoryId(matcher.group(11));
        this.setCategoryName(matcher.group(12));
        this.setHotelId(matcher.group(13));
        this.setHotelName(matcher.group(14));
        this.setRoomId(matcher.group(15));
        this.setRoomName(matcher.group(16));
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
