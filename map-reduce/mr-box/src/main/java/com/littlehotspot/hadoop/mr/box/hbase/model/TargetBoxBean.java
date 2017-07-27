package com.littlehotspot.hadoop.mr.box.hbase.model;
import com.littlehotspot.hadoop.mr.box.hbase.HBaseFamily;
import com.littlehotspot.hadoop.mr.box.hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.box.hbase.HBaseTable;
import lombok.Data;

/**
 * <h1>模型 - [目标] 机顶盒</h1>
 */
@Data
@HBaseTable(name = "box_log")
public class TargetBoxBean {

    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "attr")
    private  TargetBoxAttrBean attrBean;

}
