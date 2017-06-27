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
public class SourceMobileBean {

    /**
     * HTTP Client-IP
     */
    private String id;

    /**
     * Access-Timestamp
     */
    private String uuid;

    /**
     * HTTP-Request-Method
     */
    private String hotelId;

    /**
     * HTTP Request URI
     */
    private String roomId;

    /**
     * HTTP-Response-Status
     */
    private String timestamps;

    /**
     * HTTP-Header[referer]
     */
    private String optionType;

    /**
     * HTTP-Header[user_agent]
     */
    private String mediaType;

    /**
     * HTTP-Header[x_forwarded_for]
     */
    private String contentId;

    /**
     * Access-Time
     */
    private String categoryId;

    /**
     * 版本名称
     */
    private String mobileId;

    /**
     * 版本号
     */
    private String mediaId;

    /**
     * 手机系统版本
     */
    private String osType;

    /**
     * 系统 API 版本
     */
    private String longitude;

    /**
     * 机器型号
     */
    private String latitude;

    /**
     * 机器型号
     */
    private String serialnumber;
    /**
     * 机器型号
     */
    private String areaId;

    /**
     * 机器型号
     */
    private String commonValue;




    public SourceMobileBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setId(matcher.group(1));
        this.setUuid(matcher.group(2));
        this.setHotelId(matcher.group(3));
        this.setRoomId(matcher.group(4));
        this.setTimestamps(matcher.group(5));
        this.setOptionType(matcher.group(6));
        this.setMediaType(matcher.group(7));
        this.setContentId(matcher.group(8));
        this.setCategoryId(matcher.group(9));
        this.setMobileId(matcher.group(10));
        this.setMediaId(matcher.group(11));
        this.setOsType(matcher.group(12));
        this.setLongitude(matcher.group(13));
        this.setLatitude(matcher.group(14));
        this.setSerialnumber(matcher.group(15));
        this.setAreaId(matcher.group(16));
        this.setCommonValue(matcher.group(17));
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
