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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
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
@HBaseTable(tableName = "user_read", familyName = "attr")
public class TargetUserReadAttrBean {

    @HBaseRowKey
    private String rowKey;
    /**
     * 设备标识
     */
    @HBaseColumn(columnName = "device_id")
    private String deviceId;

    /**
     * 设备类型
     */
    @HBaseColumn(columnName = "start")
    private String start;

    /**
     * 机型
     */
    @HBaseColumn(columnName = "end")
    private String end;

    /**
     * TOKEN
     */
    @HBaseColumn(columnName = "con_id")
    private String conId;

    /**
     * 设备类型
     */
    @HBaseColumn(columnName = "con_nam")
    private String conNam;

    /**
     * 机型
     */
    @HBaseColumn(columnName = "v_time")
    private String vTime;

    /**
     * TOKEN
     */
    @HBaseColumn(columnName = "longitude")
    private String longitude;

    /**
     * 机型
     */
    @HBaseColumn(columnName = "latitude")
    private String latitude;

    /**
     * TOKEN
     */
    @HBaseColumn(columnName = "os_type")
    private String osType;



    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDeviceId() == null ? "" : this.getDeviceId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getStart() == null ? "" : this.getStart()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getEnd() == null ? "" : this.getEnd()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getConId() == null ? "" : this.getConId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getConNam() == null ? "" : this.getConNam()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getVTime() == null ? "" : this.getVTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getLongitude() == null ? "" : this.getLongitude()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getLatitude() == null ? "" : this.getLatitude()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getOsType() == null ? "" : this.getOsType()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }
}
