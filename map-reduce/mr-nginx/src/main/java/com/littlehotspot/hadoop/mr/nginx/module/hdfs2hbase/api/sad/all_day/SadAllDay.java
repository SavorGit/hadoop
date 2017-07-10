package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.all_day;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:29.
 */
@Data
@HBaseTable(name = "sad_allday_export")
public class SadAllDay {

    /**
     * rowKey
     */
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "basic")
    private TargetSadAllDayBean basicBean;

}
