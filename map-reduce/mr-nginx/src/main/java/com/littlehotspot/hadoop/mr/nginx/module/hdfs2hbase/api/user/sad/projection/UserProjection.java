package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.TextTargetSadAttrBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.TextTargetSadRelaBean;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:12.
 */
@Data
@HBaseTable(name = "user_projection")
public class UserProjection {

    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "attr")
    private TextTargetSadAttrBean attrBean;

    @HBaseFamily(name = "rela")
    private TextTargetSadRelaBean relaBean;

}
