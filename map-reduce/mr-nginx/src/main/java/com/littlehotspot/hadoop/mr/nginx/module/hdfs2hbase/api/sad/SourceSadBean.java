package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Matcher;

/**
 * <h1> 模型 - [源] 投屏点播 </h1>
 * Created by Administrator on 2017-06-19 上午 10:35.
 */
@Data
@NoArgsConstructor
public class SourceSadBean {

    /**
     * 区域id
     */
    private String areaId;

    /**
     * 区域名称
     */
    private String areaName;

    /**
     * 酒店id
     */
    private String hotelId;

    /**
     * 酒店名称
     */
    private String hotelName;

    /**
     * 包间id
     */
    private String roomId;

    /**
     * 包间名称
     */
    private String roomName;

    /**
     * 盒子id
     */
    private String boxId;

    /**
     * 盒子名称
     */
    private String boxName;

    /**
     * 盒子mac
     */
    private String boxMac;

    /**
     * 手机标识
     */
    private String mobileId;

    /**
     * 投屏数量
     */
    private String projectCount;

    /**
     * 点播数量
     */
    private String demandCount;

    /**
     * 时间（天YYYY-mm-dd,月YYYY-mm,年YYYY,所有天没有这个字段）
     */
    private String time;

    public SourceSadBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }

        this.setAreaId(cleanNull(matcher.group(1)));
        this.setAreaName(cleanNull(matcher.group(2)));
        this.setHotelId(cleanNull(matcher.group(3)));
        this.setHotelName(cleanNull(matcher.group(4)));
        this.setRoomId(cleanNull(matcher.group(5)));
        this.setRoomName(cleanNull(matcher.group(6)));
        this.setBoxId(cleanNull(matcher.group(7)));
        this.setBoxName(cleanNull(matcher.group(8)));
        this.setBoxMac(cleanNull(matcher.group(9)));
        this.setMobileId(cleanNull(matcher.group(10)));
        this.setProjectCount(cleanNull(matcher.group(11)));
        this.setDemandCount(cleanNull(matcher.group(12)));
        if(matcher.groupCount()>12) {
            this.setTime(this.cleanValue(matcher.group(13)));
        }
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    private String cleanNull(String value){
        if("\\N".equals(value)) {
            return null;
        }

        return value;
    }
}
