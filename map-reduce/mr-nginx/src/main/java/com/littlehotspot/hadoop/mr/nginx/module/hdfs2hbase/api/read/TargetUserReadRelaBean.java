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
public class TargetUserReadRelaBean {


    /**
     * 设备标识
     */
    private String deviceId;

    /**
     * 设备类型
     */
    private String start;
    /**
     * 来源
     */
    @HBaseColumn(name = "cat_id")
    private String catId;

    /**
     * 下载时间戳
     */
    @HBaseColumn(name = "cat_name")
    private String catName;

    /**
     * 首次投屏时间戳
     */
    @HBaseColumn(name = "hotel")
    private String hotel;

    /**
     * 首次点播时间戳
     */
    @HBaseColumn(name = "hotel_name")
    private String hotelName;

    /**
     * 首次阅读时间
     */
    @HBaseColumn(name = "room")
    private String room;

    /**
     * 点播总次数
     */
    @HBaseColumn(name = "room_name")
    private String roomName;



    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getCatId() == null ? "" : this.getCatId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getCatName() == null ? "" : this.getCatName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getHotel() == null ? "" : this.getHotel()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getHotelName() == null ? "" : this.getHotelName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getRoom() == null ? "" : this.getRoom()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(Constant.VALUE_SPLIT_CHAR);

        return rowLine.toString();
    }
}
