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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox;

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
public class SourceBean {


    /**
     * 区域
     */
    private String rowKey;

    /**
     * 区域
     */
    private String area;

    /**
     * 区域Id
     */
    private String areaId;


    /**
     * 酒楼ID
     */
    private String hotelId;

    /**
     * 酒楼名称
     */
    private String hotelName;


    /**
     * 包间ID
     */
    private String roomId;

    /**
     * 包间名称
     */
    private String roomName;

    /**
     *
     */
    private String tvCount;


    /**
     * 播放日期
     */
    private String playDate;

    /**
     * 机顶盒mac
     */
    private String mac;

    /**
     * 机顶盒Id
     */
    private String boxId;

    /**
     * 机顶盒名称
     */
    private String boxName;

    /**
     * 播放次数
     */
    private String playCount;

    /**
     * 播放总秒数
     */
    private String playTime;


    /**
     * 媒体类型
     */
    private String mediaType;


    /**
     * HTTP-Response-Status
     */
    private String timestamps;

    /**
     * 版本号
     */
    private String mediaId;

    /**
     * 版本号
     */
    private String mediaName;


    /**
     * 机器型号
     */
    private String commonValue;


    public SourceBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setHotelId(matcher.group(1));
        this.setHotelName(matcher.group(2));
        this.setRoomId(matcher.group(3));
        this.setRoomName(matcher.group(4));
        this.setMac(matcher.group(5));
        this.setMediaId(matcher.group(6));
        this.setMediaType(matcher.group(7));
        this.setPlayDate(matcher.group(8));
        this.setTimestamps(matcher.group(9));

    }


    public String rowLine1() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(",");
        rowLine.append(this.getHotelName() == null ? "" : this.getHotelName()).append(",");
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(",");
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getMediaId() == null ? "" : this.getMediaId()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate()).append(",");
        rowLine.append(this.getTimestamps() == null ? "" : this.getTimestamps()).append(",");
        return rowLine.toString();
    }

    public String rowLine2() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(",");
        rowLine.append(this.getAreaId() == null ? "" : this.getAreaId()).append(",");
        rowLine.append(this.getArea() == null ? "" : this.getArea()).append(",");
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(",");
        rowLine.append(this.getHotelName() == null ? "" : this.getHotelName()).append(",");
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(",");
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(",");
        rowLine.append(this.getBoxId() == null ? "" : this.getBoxId()).append(",");
        rowLine.append(this.getBoxName() == null ? "" : this.getBoxName()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getMediaId() == null ? "" : this.getMediaId()).append(",");
        rowLine.append(this.getMediaName() == null ? "" : this.getMediaName()).append(",");
        rowLine.append(this.getPlayCount() == null ? "" : this.getPlayCount()).append(",");
        rowLine.append(this.getPlayTime() == null ? "" : this.getPlayTime()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate());
        return rowLine.toString();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
