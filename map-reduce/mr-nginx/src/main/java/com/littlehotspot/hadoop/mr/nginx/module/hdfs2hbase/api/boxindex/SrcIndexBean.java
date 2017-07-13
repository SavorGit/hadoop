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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.boxindex;

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
public class SrcIndexBean {

    /**
     * 设备id
     */
    private String userId;

    /**
     *
     */
    private String mac;

    /**
     * 时间戳
     */
    private String timestamps;




    public void setValue(String text) {
        Matcher matcher = CommonVariables.MAPPER_BOX_LOG_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setUserId(matcher.group(8));
        this.setMac(matcher.group(13));
        this.setTimestamps(matcher.group(4));


    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getUserId() == null ? "" : this.getUserId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getTimestamps() == null ? "" : this.getTimestamps()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }
}
