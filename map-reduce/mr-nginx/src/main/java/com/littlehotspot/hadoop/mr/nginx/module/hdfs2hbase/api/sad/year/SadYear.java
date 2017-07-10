package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.year;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:25.
 */
@Data
@HBaseTable(name = "sad_thisyear_export")
public class SadYear {

    /**
     * rowKey
     */
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "basic")
    private TargetSadYearBean basicBean;

}
