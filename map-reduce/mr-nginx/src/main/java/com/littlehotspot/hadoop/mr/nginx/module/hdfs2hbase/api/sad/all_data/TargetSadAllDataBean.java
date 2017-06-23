package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.all_data;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.TargetSadBaseBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.HBaseTable;
import lombok.Data;

/**
 * <h1> 模型 - [目标] 投屏点播-所有天 </h1>
 * Created by Administrator on 2017-06-19 上午 10:56.
 */
@Data
@HBaseTable(tableName = "sad_alldata_export", familyName = "basic")
public class TargetSadAllDataBean extends TargetSadBaseBean {

    public TargetSadAllDataBean(String key) {
        this.setRowKey(key);
    }

}
