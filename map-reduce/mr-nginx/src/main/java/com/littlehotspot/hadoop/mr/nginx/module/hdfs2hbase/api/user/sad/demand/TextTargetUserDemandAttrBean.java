package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.demand;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 上午 10:39.
 */
@Data
@HBaseTable(tableName = "user_demand", familyName = "attr")
public class TextTargetUserDemandAttrBean {
}
