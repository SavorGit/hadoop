package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Matcher;

/**
 * <h1> 模型 - [源] box_log </h1>
 * Created by Administrator on 2017-06-23 上午 10:38.
 */
@Data
@NoArgsConstructor
public class TextSourceUserSadBean {
    /**
     * 记录标识
     */
    private String id;

    /**
     * 操作关联标识
     */
    private String uuid;

    /**
     * 酒店标识
     */
    private String hotel_id;

    /**
     * 包间标识
     */
    private String room_id;

    /**
     * 时间截
     */
    private String timestamps;

    /**
     * 操作类型
     */
    private String option_type;

    /**
     * 业务类型
     */
    private String media_type;

    /**
     * 媒体标识
     */
    private String media_id;

    /**
     * 手机标识
     */
    private String mobile_id;

    /**
     * 机顶盒APK版本
     */
    private String apk_version;

    /**
     * 广告期号
     */
    private String ads_period;

    /**
     * 点播期号
     */
    private String demand_period;

    /**
     * 通用参数值
     */
    private String common_value;

    /**
     * 机顶盒MAC
     */
    private String mac;

    /**
     * 时间 2017052812
     */
    private String date_time;

    public TextSourceUserSadBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setId(cleanNull(matcher.group(1)));
        this.setUuid(cleanNull(matcher.group(2)));
        this.setHotel_id(cleanNull(matcher.group(3)));
        this.setRoom_id(cleanNull(matcher.group(4)));
        this.setTimestamps(cleanNull(matcher.group(5)));
        this.setOption_type(cleanNull(matcher.group(6)));
        this.setMedia_type(cleanNull(matcher.group(7)));
        this.setMedia_id(cleanNull(matcher.group(8)));
        this.setMobile_id(cleanNull(matcher.group(9)));
        this.setApk_version(cleanNull(matcher.group(10)));
        this.setAds_period(cleanNull(matcher.group(11)));
        this.setDemand_period(cleanNull(matcher.group(12)));
        this.setCommon_value(cleanNull(matcher.group(13)));
        this.setMac(cleanNull(matcher.group(14)));
        this.setDate_time(cleanValue(cleanNull(matcher.group(15))));
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
