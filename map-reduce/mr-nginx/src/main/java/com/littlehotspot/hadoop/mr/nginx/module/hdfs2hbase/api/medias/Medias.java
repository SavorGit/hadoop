package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * <h1> 媒体信息表 </h1>
 * Created by Administrator on 2017-07-10 下午 2:35.
 */
@Data
@NoArgsConstructor
@HBaseTable(name = "media")
public class Medias {

    /**
     * id + rety
     */
    @HBaseRowKey
    @NonNull
    private String rowKey;

    @HBaseFamily(name = "attr")
    private TargetMediasAttrBean attrBean;

}
