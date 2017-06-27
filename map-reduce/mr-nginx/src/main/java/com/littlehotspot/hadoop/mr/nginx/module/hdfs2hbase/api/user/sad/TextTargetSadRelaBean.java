package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-27 下午 4:00.
 */
@Data
public class TextTargetSadRelaBean {
    @HBaseRowKey
    private String rowKey;

    @HBaseColumn(columnName = "hotel")
    private String hotel;

    @HBaseColumn(columnName = "hotel_name")
    private String hotel_name;

    @HBaseColumn(columnName = "room")
    private String room;

    @HBaseColumn(columnName = "room_name")
    private String room_name;

    @HBaseColumn(columnName = "box_mac")
    private String box_mac;

    @HBaseColumn(columnName = "box_name")
    private String box_name;

    @HBaseColumn(columnName = "media")
    private String media;

    @HBaseColumn(columnName = "media_name")
    private String media_name;

    @HBaseColumn(columnName = "media_down_url")
    private String media_down_url;

    @HBaseColumn(columnName = "apk_version")
    private String apk_version;

    @HBaseColumn(columnName = "ads_version")
    private String ads_version;

    @HBaseColumn(columnName = "dema_version")
    private String dema_version;
}
