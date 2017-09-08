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
@NoArgsConstructor
public class SourceRareBean {


    /**
     * 酒楼ID
     */
    private String hotelId;

    /**
     * 包间ID
     */
    private String roomId;

    /**
     * 播放日期
     */
    private String playDate;

    /**
     * 播放日期
     */
    private String date;

    /**
     * 播放日期
     */
    private String time;

    /**
     * 机顶盒mac
     */
    private String mac;

    /**
     * 播放总秒数
     */
    private String playTime;

    /**
     * 媒体类型
     *
     */
    private String mediaType;


    /**
     *
     */
    private String timestamps;

    /**
     * HTTP-Header[referer]
     */
    private String optionType;


    /**
     * Access-Time
     */
    private String categoryId;


    /**
     * 版本号
     */
    private String mediaId;



    /**
     * 机器型号
     */
    private String commonValue;

    public String rowLine1() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(",");
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(",");
        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(",");
        rowLine.append(this.getMediaId() == null ? "" : this.getMediaId()).append(",");
        rowLine.append(this.getPlayDate() == null ? "" : this.getPlayDate()).append(",");
        rowLine.append(this.getDate() == null ? "" : this.getDate()).append(",");
        rowLine.append(this.getTime() == null ? "" : this.getTime()).append(",");
        rowLine.append(this.getOptionType() == null ? "" : this.getOptionType()).append(",");
        rowLine.append(this.getMediaType() == null ? "" : this.getMediaType());
        return rowLine.toString();
    }


    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
