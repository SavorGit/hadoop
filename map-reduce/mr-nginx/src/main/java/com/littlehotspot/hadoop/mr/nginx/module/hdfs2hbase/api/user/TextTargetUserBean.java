/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:40
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;

/**
 * <h1>模型 - [目标] 用户</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月02日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@Data
@HBaseTable(tableName = "user", familyName = "basic")
public class TextTargetUserBean {

    /**
     * 设备标识
     */
    @HBaseColumn(columnName = "deviceId")
    @HBaseRowKey
    private String deviceId;

    /**
     * 设备类型
     */
    @HBaseColumn(columnName = "deviceType")
    private String deviceType;

    /**
     * 机型
     */
    @HBaseColumn(columnName = "machineModel")
    private String machineModel;

    /**
     * TOKEN
     */
    @HBaseColumn(columnName = "token")
    private String token;

    /**
     * 下载时间
     */
    @HBaseColumn(columnName = "downloadTime")
    private String downloadTime;

    /**
     * 来源
     */
    @HBaseColumn(columnName = "since")
    private String since;

    /**
     * 首次投屏时间
     */
    @HBaseColumn(columnName = "projectionTime")
    private String projectionTime;

    /**
     * 首次点播时间
     */
    @HBaseColumn(columnName = "demandTime")
    private String demandTime;

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getDeviceId() == null ? "" : this.getDeviceId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDeviceType() == null ? "" : this.getDeviceType()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMachineModel() == null ? "" : this.getMachineModel()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getToken() == null ? "" : this.getToken()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getSince() == null ? "" : this.getSince()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDownloadTime() == null ? "" : this.getDownloadTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getProjectionTime() == null ? "" : this.getProjectionTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDemandTime() == null ? "" : this.getDemandTime()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }
}
