package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import lombok.Data;

/**
 * <h1> 用户行为最终表BaseBean（rela列簇） </h1>
 * Created by Administrator on 2017-06-27 下午 4:00.
 */
@Data
public class TextTargetSadRelaBean {

    @HBaseColumn(name = "hotel")
    private String hotel;

    @HBaseColumn(name = "hotel_name")
    private String hotel_name;

    @HBaseColumn(name = "room")
    private String room;

    @HBaseColumn(name = "room_name")
    private String room_name;

    @HBaseColumn(name = "box_mac")
    private String box_mac;

    @HBaseColumn(name = "box_name")
    private String box_name;

    @HBaseColumn(name = "media")
    private String media;

    @HBaseColumn(name = "media_name")
    private String media_name;

    @HBaseColumn(name = "media_down_url")
    private String media_down_url;

    @HBaseColumn(name = "apk_version")
    private String apk_version;

    @HBaseColumn(name = "ads_version")
    private String ads_version;

    @HBaseColumn(name = "dema_version")
    private String dema_version;
}
