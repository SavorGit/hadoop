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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.contentdetail;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox.CommonVariables;
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
     *
     */
    private String uuid;


    /**
     *
     */
    private String mobileId;

    /**
     *
     */
    private String contentId;

    /**
     *
     */
    private String contentName;


    /**
     *
     */
    private String categoryId;

    /**
     *
     */
    private String categoryName;


    /**
     *
     */
    private String commonValue;

    /**
     *
     */
    private String timestamps;

    /**
     *
     */
    private String optionType;


    /**
     *
     */
    private String mediaType;

    /**
     *
     */
    private String operators;

    /**
     *
     */
    private String createTime;

    /**
     *
     */
    private String dateTime;

    /**
     *
     */
    private String readCount;

    /**
     *
     */
    private String readDuration;


    /**
     *
     *
     */
    private String shareCount;


    /**
     *
     */
    private String pvCount;

    /**
     * 版本号
     */
    private String uvCount;

    /**
     * 版本号
     */
    private String clickCount;

    /**
     * 版本号
     */
    private String vvCount;

    /**
     * 版本号
     */
    private String demandCount;

    /**
     * 版本号
     */
    private String outlineCount;



    public String rowLine1() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getUuid() == null ? "" : this.getUuid()).append(",");
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getOptionType() == null ? "" : this.getOptionType()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType()).append(",");
        rowLine.append(this.getTimestamps() == null ? "" : this.getTimestamps()).append(",");
        rowLine.append(this.getCommonValue() == null ? "" : this.getCommonValue()).append(",");
        rowLine.append(this.getDateTime() == null ? "" : this.getDateTime());
        return rowLine.toString();
    }

    public String rowLine2() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getContentName() == null ? "" : this.getContentName()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getCategoryName() == null ? "" : this.getCategoryName()).append(",");
        rowLine.append(this.getCommonValue() == null ? "" : this.getCommonValue()).append(",");
        rowLine.append(this.getDateTime() == null ? "" : this.getDateTime()).append(",");
        rowLine.append(this.getReadCount() == null ? "" : this.getReadCount()).append(",");
        rowLine.append(this.getShareCount() == null ? "" : this.getShareCount()).append(",");
        rowLine.append(this.getPvCount() == null ? "" : this.getPvCount()).append(",");
        rowLine.append(this.getClickCount() == null ? "" : this.getClickCount()).append(",");
        rowLine.append(this.getVvCount() == null ? "" : this.getVvCount()).append(",");
        rowLine.append(this.getOutlineCount() == null ? "" : this.getOutlineCount()).append(",");
        rowLine.append(this.getOperators() == null ? "" : this.getOperators()).append(",");
        rowLine.append(this.getCreateTime() == null ? "" : this.getCreateTime());
        return rowLine.toString();
    }

    public String rowLine3() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getMobileId() == null ? "" : this.getMobileId()).append(",");
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getOptionType() == null ? "" : this.getOptionType()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType()).append(",");
        rowLine.append(this.getDateTime() == null ? "" : this.getDateTime());
        return rowLine.toString();
    }

    public String rowLine4() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getMobileId() == null ? "" : this.getMobileId()).append(",");
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getDateTime() == null ? "" : this.getDateTime());
        return rowLine.toString();
    }

    public String rowLine5() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getDateTime() == null ? "" : this.getDateTime()).append(",");
        rowLine.append(this.getUvCount() == null ? "" : this.getUvCount()).append(",");
        rowLine.append("uv");

        return rowLine.toString();
    }

    public String rowLine6() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getContentId() == null ? "" : this.getContentId()).append(",");
        rowLine.append(this.getCategoryId() == null ? "" : this.getCategoryId()).append(",");
        rowLine.append(this.getDateTime() == null ? "" : this.getDateTime()).append(",");
        rowLine.append(this.getReadDuration() == null ? "" : this.getReadDuration()).append(",");
        rowLine.append("duration");

        return rowLine.toString();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
