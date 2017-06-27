package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-26 下午 4:55.
 */
@Data
@NoArgsConstructor
public class TextTargetSadActBean {

    @HBaseRowKey
    private String rowKey;

    /**
     * 操作关联标识
     */
    @HBaseColumn(columnName = "uuid")
    private String uuid;

    /**
     * 酒店标识
     */
    @HBaseColumn(columnName = "hotel_id")
    private String hotel_id;

    /**
     * 包间标识
     */
    @HBaseColumn(columnName = "room_id")
    private String room_id;

    /**
     * 时间截
     */
    @HBaseColumn(columnName = "timestamps")
    private String timestamps;

    /**
     * 操作类型
     */
    @HBaseColumn(columnName = "option_type")
    private String option_type;

    /**
     * 业务类型
     */
    @HBaseColumn(columnName = "media_type")
    private String media_type;

    /**
     * 媒体标识
     */
    @HBaseColumn(columnName = "media_id")
    private String media_id;

    /**
     * 手机标识
     */
    @HBaseColumn(columnName = "mobile_id")
    private String mobile_id;

    /**
     * 机顶盒APK版本
     */
    @HBaseColumn(columnName = "apk_version")
    private String apk_version;

    /**
     * 广告期号
     */
    @HBaseColumn(columnName = "ads_period")
    private String ads_period;

    /**
     * 点播期号
     */
    @HBaseColumn(columnName = "demand_period")
    private String demand_period;

    /**
     * 通用参数值
     */
    @HBaseColumn(columnName = "common_value")
    private String common_value;

    /**
     * 机顶盒MAC
     */
    @HBaseColumn(columnName = "mac")
    private String mac;

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getUuid() == null ? "" : this.getUuid()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getHotel_id() == null ? "" : this.getHotel_id()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getRoom_id() == null ? "" : this.getRoom_id()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getTimestamps() == null ? "" : this.getTimestamps()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getOption_type() == null ? "" : this.getOption_type()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMedia_type() == null ? "" : this.getMedia_type()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMedia_id() == null ? "" : this.getMedia_id()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMobile_id() == null ? "" : this.getMobile_id()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getApk_version() == null ? "" : this.getApk_version()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getAds_period() == null ? "" : this.getAds_period()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDemand_period() == null ? "" : this.getDemand_period()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getCommon_value() == null ? "" : this.getCommon_value()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMac() == null ? "" : this.getMac());
        return rowLine.toString();
    }

    public TextTargetSadActBean(String text) {
        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX_FINAL.matcher(text);
        if (!matcher.find()) {
            return;
        }
        this.setRowKey(cleanNull(matcher.group(1)));
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
        this.setMac(cleanValue(cleanNull(matcher.group(14))));
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
