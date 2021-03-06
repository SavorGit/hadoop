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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read.CommonVariables;
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
public class SourceRareBean {

    /**
     * 区域
     */
    private String area;


    /**
     * 酒楼ID
     */
    private String hotelId;

    /**
     * 酒楼名称
     */
    private String hotelName;


    /**
     * 机顶盒位置
     */
    private String addr;
    /**
     * 包间ID
     */
    private String roomId;

    /**
     * 包间名称
     */
    private String roomName;

    /**
     * 维护人
     *
     */
    private String maintenMan;

    /**
     * 重点酒楼
     */
    private String isKey;

    /**
     * 播放日期
     */
    private String playDate;

    /**
     * 机顶盒mac
     */
    private String mac;

    /**
     * 播放次数
     */
    private String playCount;

    /**
     * 播放总秒数
     */
    private String playTime;

    /**
     * 开机率
     *
     */
    private String production;

    /**
     * 媒体类型
     *
     */
    private String mediaType;


    /**
     * HTTP-Response-Status
     */
    private String timestamps;

    /**
     * HTTP-Header[referer]
     */
    private String optionType;


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
     * 机器型号
     */
    private String commonValue;

    public String rowLine1() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(",");
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(",");
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getMediaId() == null ? "" : this.getMediaId()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType()).append(",");
        return rowLine.toString();
    }

    public String rowLine2() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getArea() == null ? "" : this.getArea()).append(",");
        rowLine.append(this.getHotelName() == null ? "" : this.getHotelName()).append(",");
        rowLine.append(this.getAddr() == null ? "" : this.getAddr()).append(",");
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(",");
        rowLine.append(this.getMaintenMan() == null ? "" : this.getMaintenMan()).append(",");
        rowLine.append(this.getIsKey() == null ? "" : this.getIsKey()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getPlayCount() == null ? "" : this.getPlayCount()).append(",");
        rowLine.append(this.getPlayTime() == null ? "" : this.getPlayTime()).append(",");
        rowLine.append(this.getProduction() == null ? "" : this.getProduction());
        return rowLine.toString();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
