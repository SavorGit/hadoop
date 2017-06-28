package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.demand;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.TextTargetSadRelaBean;
import lombok.Data;

/**
 * <h1> 用户点播明细表（rela列簇） </h1>
 * Created by Administrator on 2017-06-23 上午 10:39.
 */
@Data
@HBaseTable(tableName = "user_demand", familyName = "rela")
public class TextTargetUserDemandRelaBean extends TextTargetSadRelaBean{
}
