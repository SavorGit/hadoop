package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import lombok.Data;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 * <h1> mysql查询model </h1>
 * Created by Administrator on 2017-06-30 上午 11:02.
 */
@Data
public class SelectModel {

    /**
     * 查询model
     */
    private Class<? extends DBWritable> inputClass;

    /**
     * 查询表名
     */
    private String tableName;

    /**
     * 查询条件
     */
    private String conditions;

    /**
     * 排序条件
     */
    private String orderBy;

    /**
     * 查询返回的字段
     */
    private String[] fields;

    /**
     * 读取的输出路径
     */
    private String output;

}
