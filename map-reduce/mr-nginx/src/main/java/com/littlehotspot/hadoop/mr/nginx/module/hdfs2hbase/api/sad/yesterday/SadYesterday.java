package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.yesterday;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:19.
 */
@Data
@HBaseTable(name = "sad_yesterday_export")
public class SadYesterday {

    /**
     * rowKey
     */
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "basic")
    private TargetSadYesterdayBean basicBean;

}
