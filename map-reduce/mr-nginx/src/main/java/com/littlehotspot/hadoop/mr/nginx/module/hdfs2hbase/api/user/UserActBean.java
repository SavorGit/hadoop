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
public class UserActBean {

    /**
     * 用户Id
     */
    private String deviceId;

    /**
     * 首次操作时间
     */
    private String time;

    /**
     * 操作次数
     */
    private String count;



    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getDeviceId() == null ? "" : this.getDeviceId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getTime() == null ? "" : this.getTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getCount() == null ? "" : this.getCount()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
