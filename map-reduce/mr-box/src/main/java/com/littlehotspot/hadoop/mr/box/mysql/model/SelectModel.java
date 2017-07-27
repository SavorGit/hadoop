package com.littlehotspot.hadoop.mr.box.mysql.model;

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
     * 读取的输出路径
     */
    private String outputPath;

    /**
     * sql语句
     */
    private String query;

    /**
     * 查询记录条数
     */
    private String countQuery;

}
