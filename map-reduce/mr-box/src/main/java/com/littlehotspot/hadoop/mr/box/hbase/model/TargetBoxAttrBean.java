package com.littlehotspot.hadoop.mr.box.hbase.model;
import com.littlehotspot.hadoop.mr.box.hbase.HBaseColumn;
import lombok.Data;

/**
 * <h1>模型 - [目标] 机顶盒</h1>
 */
@Data
public class TargetBoxAttrBean {
    /**
     * uuid
     */
    @HBaseColumn(name = "uuid")
    private String uuid;
    /**
     * 酒店id
     */
    @HBaseColumn(name = "hotel_id")
    private String hotelId;
    /**
     * 包间id
     */
    @HBaseColumn(name = "room_id")
    private String roomId;
    /**
     * 时间戳
     */
    @HBaseColumn(name = "timestamps")
    private String timestamps;
    /**
     * 操作类型
     */
    @HBaseColumn(name = "option_type")
    private String optionType;
    /**
     * 媒体类型
     */
    @HBaseColumn(name = "mda_type")
    private String mdaType;
    /**
     * 媒体id
     */
    @HBaseColumn(name = "mda_id")
    private String mdaId;
    /**
     * 手机id
     */
    @HBaseColumn(name = "mobile_id")
    private String mobileId;
    /**
     * apk 版本
     */
    @HBaseColumn(name = "apk_version")
    private String apkVersion;
    /**
     * 广告视频期号
     */
    @HBaseColumn(name = "ads_period")
    private String adsPeriod;
    /**
     * 点播期号
     */
    @HBaseColumn(name = "dmd_period")
    private String dmdPeriod;
    /**
     * 机顶盒mac
     */
    @HBaseColumn(name = "mac")
    private String mac;
    /**
     * 生成日期
     */
    @HBaseColumn(name = "date_time")
    private String dateTime;
    /**
     * 酒楼名称
     */
    @HBaseColumn(name = "hotel_name")
    private String hotelName;
    /**
     * 包间名称
     */
    @HBaseColumn(name = "room_name")
    private String roomName;
    /**
     * 媒体名称
     */
    @HBaseColumn(name = "mda_name")
    private String mediaName;

    /**
     * 通用参数
     */
    @HBaseColumn(name = "com_value")
    private String customVolume;
}
