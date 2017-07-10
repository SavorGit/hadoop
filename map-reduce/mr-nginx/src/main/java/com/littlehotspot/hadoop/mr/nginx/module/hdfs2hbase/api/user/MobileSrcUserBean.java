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
     * 设备id
     */
    private String deviceId;

    /**
     * 用户名
     */
    private String username;

    /**
     * 性别
     */
    private String gender;

    /**
     * 年龄
     */
    private String age;

    /**
     * 手机类型
     */
    private String mType;

    /**
     * 手机机型
     */
    private String mMachine;
    /**
     * token
     */
    private String token;

    /**
     * 首次下载来源
     */
    private String fDownSrc;

    /**
     * 首次下载时间
     */
    private String fDownTime;

    /**
     * 首次点播时间
     */
    private String fDemaTime;

    /**
     * 首次投屏时间
     */
    private String fProjeTime;

    /**
     * 首次阅读时间
     */
    private String fReadTime;

    /**
     * 点播总次数
     */
    private String demaCount;

    /**
     * 投屏总次数
     */
    private String projeCount;

    /**
     * 阅读总次数
     */
    private String readCount;



    public void setValue(String text) {
        Matcher matcher = CommonVariables.MAPPER_MOBILE_LOG_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setDeviceId(matcher.group(9));
        if (StringUtils.isBlank(this.getFDownTime())||(!StringUtils.isBlank(matcher.group(4))&&Long.valueOf(this.getFDownTime())>Long.valueOf(matcher.group(4)))){
            this.setFDownTime(matcher.group(4));
            this.setFDownSrc("mob");
        }



    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getDeviceId() == null ? "" : this.getDeviceId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMType() == null ? "" : this.getMType()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMMachine() == null ? "" : this.getMMachine()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getFDownTime() == null ? "" : this.getFDownTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getFDownSrc() == null ? "" : this.getFDownSrc()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getToken() == null ? "" : this.getToken()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }
}
