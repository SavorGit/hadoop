package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.TextTargetSadActBean;
import lombok.Data;

/**
 * <h1> 投屏开始 </h1>
 * Created by Administrator on 2017-06-26 下午 4:52.
 */
@Data
@HBaseTable(tableName = "user_projection_start", familyName = "basic")
public class TargetUserProjectionStartBean extends TextTargetSadActBean {

}
