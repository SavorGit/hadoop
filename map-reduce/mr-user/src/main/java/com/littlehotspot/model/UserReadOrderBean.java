package com.littlehotspot.model;

import lombok.Data;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseFamily;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseRowKey;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseTable;

/**
 * @Author 刘飞飞
 * @Date 2017/7/21 11:20
 */
@Data
@HBaseTable(name = "user_read_order")
public class UserReadOrderBean {
    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "attr")
    private UserReadOrderAttrBean userReadOrderAttrBean;
}
