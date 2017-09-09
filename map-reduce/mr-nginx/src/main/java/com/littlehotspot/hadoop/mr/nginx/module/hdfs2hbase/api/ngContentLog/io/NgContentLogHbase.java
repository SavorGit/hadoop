package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.io;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.CommonVariables;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseFamily;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseRowKey;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseTable;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-07 下午 3:49.
 */
@Data
@NoArgsConstructor
@HBaseTable(name = CommonVariables.HBASE_TABLE_NAME)
public class NgContentLogHbase {

    /**
     * id + rety
     */
    @HBaseRowKey
    @NonNull
    private String rowKey;

    @HBaseFamily(name = "attr")
    private NgContentLogAttrBean attrBean;

}
