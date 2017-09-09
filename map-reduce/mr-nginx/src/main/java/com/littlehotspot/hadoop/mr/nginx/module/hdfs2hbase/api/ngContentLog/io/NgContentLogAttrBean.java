package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.io;

import lombok.Data;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseColumn;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-07 下午 3:57.
 */
@Data
public class NgContentLogAttrBean {

    private static final char FIELD_DELIMITER = 0x0001;

    @HBaseColumn(name = "ip")
    private String ip;

    @HBaseColumn(name = "is_wx")
    private String isWx;

    @HBaseColumn(name = "net_type")
    private String netType;

    @HBaseColumn(name = "device_type")
    private String deviceType;

    @HBaseColumn(name = "timestamp")
    private long timestamp;

    @HBaseColumn(name = "content_id")
    private String contentId;

    @HBaseColumn(name = "channel")
    private String channel;

    @HBaseColumn(name = "is_sq")
    private String isSq;

    @HBaseColumn(name = "request_url")
    private String requestUrl;

    @Override
    public String toString() {
        StringBuffer toString = new StringBuffer();
        toString.append(this.ip).append(FIELD_DELIMITER);
        toString.append(this.isWx).append(FIELD_DELIMITER);
        toString.append(this.netType).append(FIELD_DELIMITER);
        toString.append(this.deviceType).append(FIELD_DELIMITER);
        toString.append(this.timestamp).append(FIELD_DELIMITER);
        toString.append(this.contentId).append(FIELD_DELIMITER);
        toString.append(this.channel).append(FIELD_DELIMITER);
        toString.append(this.isSq).append(FIELD_DELIMITER);
        toString.append(this.requestUrl);

        return toString.toString();
    }
}
