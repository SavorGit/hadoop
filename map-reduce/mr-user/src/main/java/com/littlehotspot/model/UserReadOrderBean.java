package com.littlehotspot.model;

import com.littlehotspot.util.hbase.HBaseFamily;
import com.littlehotspot.util.hbase.HBaseRowKey;
import com.littlehotspot.util.hbase.HBaseTable;
import lombok.Data;

/**
 *@Author 刘飞飞
 *@Date 2017/7/21 11:20
 */
@Data
@HBaseTable(name = "user_read_order")
public class UserReadOrderBean {
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "attr")
    private UserReadOrderAttrBean userReadOrderAttrBean;
}
