package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.TextTargetSadRelaBean;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-26 下午 3:31.
 */
@Data
@HBaseTable(tableName = "user_projection", familyName = "rela")
public class TextTargetUserProjectionRelaBean extends TextTargetSadRelaBean {

}
