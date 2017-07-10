package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:14.
 */
@Data
@NoArgsConstructor
@HBaseTable(name = "resources")
public class Resources {

    /**
     * id + rety
     */
    @HBaseRowKey
    @NonNull
    private String rowKey;

    @HBaseFamily(name = "attr")
    private TargetResourcesAttrBean attrBean;

    @HBaseFamily(name = "adat")
    private TargetResourcesAdatBean adatBean;

}
