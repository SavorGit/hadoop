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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.tags;

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
@HBaseTable(tableName = "tags", familyName = "attr")
public class TargetTagAttrBean {

    @HBaseRowKey
    private String rowKey;
    /**
     * 设备标识
     */
    @HBaseColumn(columnName = "name")
    private String name;

    /**
     * 设备类型
     */
    @HBaseColumn(columnName = "resources")
    private String resources;




    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getName() == null ? "" : this.getName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getResources() == null ? "" : this.getResources()).append(Constant.VALUE_SPLIT_CHAR);

        return rowLine.toString();
    }
}
