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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.nginx;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.CommonVariables;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;

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
public class MobileSrcUserBean {

    /**
     * uuid
     */
    private String uuid;

    /**
     * 酒楼id
     */
    private String hotelId;

    /**
     * 包间id
     */
    private String roomId;

    /**
     * 时间戳
     */
    private String timestamps;

    /**
     * 操作类型
     */
    private String optionType;

    /**
     * 媒体类型
     */
    private String mediaType;
    /**
     * 内容id
     */
    private String contentId;

    /**
     * 分类id
     */
    private String categoryId;

    /**
     * 手机id
     */
    private String mobileId;

    /**
     * 媒体id
     */
    private String mediaId;

    /**
     * 系统类型
     */
    private String osType;

    /**
     * 经度
     */
    private String longitude;

    /**
     * 纬度
     */
    private String latitude;

    /**
     * 序列号
     */
    private String serialnumber;

    /**
     * 区域id
     */
    private String areaId;

    /**
     * 通用值
     */
    private String commonValue;

    /**
     * token
     */
    private String token;

    /**
     * 设备型号
     */
    private String deviceModel;



    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getUuid() == null ? "" : this.getUuid()).append(",");
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(",");
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(",");
        rowLine.append(this.getTimestamps() == null ? "" : this.getTimestamps()).append(",");
        rowLine.append(this.getOptionType() == null ? "" : this.getOptionType()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType()).append(",");
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getMobileId() == null ? "" : this.getMobileId()).append(",");
        rowLine.append(this.getMediaId() == null ? "" : this.getMediaId()).append(",");
        rowLine.append(this.getOsType() == null ? "" : this.getOsType()).append(",");
        rowLine.append(this.getLongitude() == null ? "" : this.getLongitude()).append(",");
        rowLine.append(this.getLatitude() == null ? "" : this.getLatitude()).append(",");
        rowLine.append(this.getSerialnumber() == null ? "" : this.getSerialnumber()).append(",");
        rowLine.append(this.getAreaId() == null ? "" : this.getAreaId()).append(",");
        rowLine.append(this.getCommonValue() == null ? "" : this.getCommonValue()).append(",");
        rowLine.append(this.getToken() == null ? "" : this.getToken()).append(",");
        rowLine.append(this.getDeviceModel() == null ? "" : this.getDeviceModel());
        return rowLine.toString();
    }
}
