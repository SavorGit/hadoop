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
public class TotalTargetRareBean {

    /**
     * 区域
     */
    @HBaseColumn(name = "area")
    private String area;


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
     * 机顶盒mac
     */
    @HBaseColumn(name = "mac")
    private String mac;

    /**
     * 播放总秒数
     */
    @HBaseColumn(name = "play_count")
    private String playCount;

    /**
     * 播放总秒数
     */
    @HBaseColumn(name = "production")
    private String production;

    /**
     * 开机率
     *
     */
    @HBaseColumn(name = "av_production")
    private String avProduction;


    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
