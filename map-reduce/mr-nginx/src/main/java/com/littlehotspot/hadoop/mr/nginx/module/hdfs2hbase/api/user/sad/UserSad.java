package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 12:20.
 */
@Data
public class UserSad {

    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "attr")
    private TextTargetSadAttrBean attrBean;

    @HBaseFamily(name = "rela")
    private TextTargetSadRelaBean relaBean;
}
