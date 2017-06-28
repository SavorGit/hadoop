package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import lombok.Data;

/**
 * <h1> 用户行为最终表BaseBean（attr列簇） </h1>
 * Created by Administrator on 2017-06-26 下午 4:55.
 */
@Data
public class TextTargetSadAttrBean {

    @HBaseRowKey
    private String rowKey;

    @HBaseColumn(columnName = "device_id")
    private String device_id;

    @HBaseColumn(columnName = "start")
    private long start;

    @HBaseColumn(columnName = "end")
    private long end;

    @HBaseColumn(columnName = "type")
    private String type;

    @HBaseColumn(columnName = "p_time")
    private long p_time;

    public void setP_time() {
        this.p_time = this.end-this.start;
    }

    public void setStart(long start) {
        this.start = start;
        this.setP_time();
    }

    public void setEnd(long end) {
        this.end = end;
        this.setP_time();
    }

}
