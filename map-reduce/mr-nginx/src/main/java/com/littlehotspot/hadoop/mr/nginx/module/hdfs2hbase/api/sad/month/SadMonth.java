package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.month;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:28.
 */
@Data
@HBaseTable(name = "sad_thismonth_export")
public class SadMonth {

    /**
     * rowKey
     */
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "basic")
    private TargetSadMonthBean basicBean;

}
