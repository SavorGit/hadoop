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
public class TargetUserActiBean {

    /**
     * 来源
     */
    @HBaseColumn(name = "f_down_src")
    private String since;

    /**
     * 下载时间戳
     */
    @HBaseColumn(name = "f_down_time")
    private String downloadTime;

    /**
     * 首次投屏时间戳
     */
    @HBaseColumn(name = "f_proje_time")
    private String projectionTime;

    /**
     * 首次点播时间戳
     */
    @HBaseColumn(name = "f_dema_time")
    private String demandTime;

    /**
     * 首次阅读时间
     */
    @HBaseColumn(name = "f_read_time")
    private String readTime;

    /**
     * 点播总次数
     */
    @HBaseColumn(name = "dema_count")
    private String demandCount;

    /**
     * 投屏总次数
     */
    @HBaseColumn(name = "proje_count")
    private String projectionCount;

    /**
     * 阅读总次数
     */
    @HBaseColumn(name = "read_count")
    private String readCount;

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getSince() == null ? "" : this.getSince()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDownloadTime() == null ? "" : this.getDownloadTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getProjectionTime() == null ? "" : this.getProjectionTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDemandTime() == null ? "" : this.getDemandTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getReadTime() == null ? "" : this.getReadTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDemandCount() == null ? "" : this.getDemandCount()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getProjectionCount() == null ? "" : this.getProjectionCount()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getReadCount() == null ? "" : this.getReadTime()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }
}
