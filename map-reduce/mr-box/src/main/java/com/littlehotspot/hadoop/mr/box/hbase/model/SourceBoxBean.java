package com.littlehotspot.hadoop.mr.box.hbase.model;

import com.littlehotspot.hadoop.mr.box.common.CommonVariables;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Matcher;

/**
 * <h1>模型 - [源] 机顶盒</h1>
 */
@Data
@NoArgsConstructor
public class SourceBoxBean {
    /**
     * uuid
     */
    private String uuid;
    /**
     * 酒店id
     */
    private String hotelId;
    /**
     * 包间id
     */
    private String roomId;
    /**
     * 时间戳
     */
    private String timestamps;
    /**
     * 操作类型
     */
    private String optionType;
    /**
     * 媒体类型
     */
    private String mdaType;
    /**
     * 媒体id
     */
    private String mdaId;
    /**
     * 手机id
     */
    private String mobileId;
    /**
     * apk 版本
     */
    private String apkVersion;
    /**
     * 广告视频期号
     */
    private String adsPeriod;
    /**
     * 点播期号
     */
    private String dmdPeriod;
    /**
     * 通用参数
     */
    private String customVolume;
    /**
     * 机顶盒mac
     */
    private String mac;
    /**
     * 生成日期
     */
    private String dateTime;
    /**
     * 酒楼名称
     */
    private String hotelName;
    /**
     * 包间名称
     */
    private String roomName;
    /**
     * 媒体名称
     */
    private String mediaName;

    public SourceBoxBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_LOG_INTEGRATED_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setUuid(matcher.group(1));
        this.setHotelId(matcher.group(2));
        this.setRoomId(matcher.group(3));
        this.setTimestamps(matcher.group(4));
        this.setOptionType(matcher.group(5));
        this.setMdaType(matcher.group(6));
        this.setMdaId(matcher.group(7));
        this.setMobileId(matcher.group(8));
        this.setApkVersion(matcher.group(9));
        this.setAdsPeriod(matcher.group(10));
        this.setDmdPeriod(matcher.group(11));
        this.setCustomVolume(matcher.group(12));
        this.setMac(matcher.group(13));
        this.setDateTime(matcher.group(14));
        this.setHotelName(matcher.group(15));
        this.setRoomName(matcher.group(16));
        this.setMediaName(matcher.group(17));
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

}
