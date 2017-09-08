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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.zhengwei;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import lombok.Data;

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
     * 酒楼ID
     */
    @HBaseColumn(name = "hotel_id")
    private String hotelId;

    /**
     * 包间ID
     */
    @HBaseColumn(name = "room_id")
    private String roomId;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "option_type")
    private String optionType;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "media_type")
    private String mediaType;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "content_name")
    private String contentName;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "play_date")
    private String playDate;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "date")
    private String date;

    /**
     * 播放日期
     */
    @HBaseColumn(name = "time")
    private String time;

    /**
     * 机顶盒mac
     */
    @HBaseColumn(name = "mac")
    private String mac;

    /**
     * 播放秒数
     */
    @HBaseColumn(name = "play_time")
    private String playTime;



    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(",");
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(",");
        rowLine.append(this.getDate() == null ? "" : this.getDate()).append(",");
        rowLine.append(this.getTime() == null ? "" : this.getTime()).append(",");
        rowLine.append(this.getOptionType() == null ? "" : this.getOptionType()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType()).append(",");
        rowLine.append(this.getContentName() == null ? "" : this.getContentName()).append(",");
        rowLine.append(this.getPlayTime() == null ? "" : this.getPlayDate()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate());
        return rowLine.toString();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
