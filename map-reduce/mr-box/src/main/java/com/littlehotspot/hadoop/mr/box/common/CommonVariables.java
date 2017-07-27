package com.littlehotspot.hadoop.mr.box.common;

import com.littlehotspot.hadoop.mr.box.mysql.JDBCTool;

import java.util.regex.Pattern;

/**
 * 公共变量 - 数据格式转换
 */
public class CommonVariables{
    /**
     * Mapper 输入时正则过滤
     */
    public static Pattern MAPPER_LOG_FORMAT_REGEX = Pattern.compile("^(\\d+),(\\d*),(\\d*),(\\d+),(.*),(.*),(.*),(\\d*),([\\d,\\.]*),(\\d*),(\\d*),(.*),([A-Z,0-9]+),(.*)$");
    /**
     * 整合数据正则
     */
    public static Pattern MAPPER_LOG_INTEGRATED_FORMAT_REGEX = Pattern.compile("^(\\d+),(\\d*),(\\d*),(\\d+),(.*),(.*),(.*),(\\d*),([\\d,\\.]*),(\\d*),(\\d*),(.*),([A-Z,0-9]+),(.*),(.*),(.*),(.*)$");

    public static JDBCTool jdbcTool;

}
