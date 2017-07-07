package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.all_data;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.all_day.TargetSadAllDayBean;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:31.
 */
@Data
@HBaseTable(name = "sad_alldata_export")
public class SadAllData {

    /**
     * rowKey
     */
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "basic")
    private TargetSadAllDataBean basicBean;

}
