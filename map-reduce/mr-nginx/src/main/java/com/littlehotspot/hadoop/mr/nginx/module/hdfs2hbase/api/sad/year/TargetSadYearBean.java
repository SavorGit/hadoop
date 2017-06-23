package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.year;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.TargetSadBaseBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.HBaseTable;
import lombok.Data;

/**
 * <h1> 模型 - [目标] 投屏点播-所有天 </h1>
 * Created by Administrator on 2017-06-19 上午 10:56.
 */
@Data
@HBaseTable(tableName = "sad_thisyear_export", familyName = "basic")
public class TargetSadYearBean extends TargetSadBaseBean {

    /**
     * 时间（天YYYY-mm-dd,月YYYY-mm,年YYYY,所有天没有这个字段）
     */
    @HBaseColumn(columnName = "time")
    private String time;

    public TargetSadYearBean(String key) {
        this.setRowKey(key);
    }

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer(super.rowLine());
        rowLine.append(this.getTime() == null ? "" : this.getTime());
        return rowLine.toString();
    }


}
