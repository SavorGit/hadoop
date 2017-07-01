package com.littlehotspot.hadoop.mr.nginx.mysql;

import com.littlehotspot.hadoop.mr.nginx.mysql.model.Model;

import java.util.HashMap;
import java.util.Map;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-30 上午 10:06.
 */
public class MysqlCommonVariables {

    public static String dbUrl = "jdbc:mysql://rr-2zevja6lfg5718e3ko.mysql.rds.aliyuncs.com:3306/cloud?useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&zeroDateTimeBehavior=convertToNull";

    public static String userName = "java_api_read";

    public static String passwd = "KESs23DRZVX7hrqe";

    /**
     * 查询结果
     */
    public static Map<String,Model> modelMap = new HashMap<>();

    /**
     * 酒店查询字段
     */
    public static String[] hotelFields = {"id", "name"};

    /**
     * 酒店查询字段
     */
    public static String[] roomFields = {"id", "name"};

    /**
     * 酒店查询字段
     */
    public static String[] boxFields = {"id", "name", "mac"};

    /**
     * 酒店查询字段
     */
    public static String[] mediaFields = {"id", "name","oss_addr"};

}
