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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import lombok.Data;
import lombok.NoArgsConstructor;

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
public class TargetRareBean {

    /**
     * 区域
     */
    @HBaseColumn(name = "area")
    private String area;


    /**
     * 酒楼ID
     */
    @HBaseColumn(name = "hotel_id")
    private String hotelId;

    /**
     * 酒楼名称
     */
    @HBaseColumn(name = "hotel_name")
    private String hotelName;


    /**
     * 机顶盒位置
     */
    @HBaseColumn(name = "addr")
    private String addr;
    /**
     * 包间ID
     */
    @HBaseColumn(name = "room_id")
    private String roomId;

    /**
     * 包间名称
     */
    @HBaseColumn(name = "room_name")
    private String roomName;

    /**
     * 维护人
     *
     */
    @HBaseColumn(name = "mainten_man")
    private String maintenMan;

    /**
     * 重点酒楼
     */
    @HBaseColumn(name = "isKey")
    private String isKey;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "play_date")
    private String playDate;

    /**
     * 机顶盒mac
     */
    @HBaseColumn(name = "mac")
    private String mac;

    /**
     * 播放次数
     */
    @HBaseColumn(name = "play_count")
    private String playCount;

    /**
     * 播放总秒数
     */
    @HBaseColumn(name = "play_time")
    private String playTime;

    /**
     * 开机率
     *
     */
    @HBaseColumn(name = "production")
    private String production;


    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getArea() == null ? "" : this.getArea()).append(",");
        rowLine.append(this.getHotelName() == null ? "" : this.getHotelName()).append(",");
        rowLine.append(this.getAddr() == null ? "" : this.getAddr()).append(",");
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(",");
        rowLine.append(this.getMaintenMan() == null ? "" : this.getMaintenMan()).append(",");
        rowLine.append(this.getIsKey() == null ? "" : this.getIsKey()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getPlayCount() == null ? "" : this.getPlayCount()).append(",");
        rowLine.append(this.getPlayTime() == null ? "" : this.getPlayTime()).append(",");
        rowLine.append(this.getProduction() == null ? "" : this.getProduction());
        return rowLine.toString();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
