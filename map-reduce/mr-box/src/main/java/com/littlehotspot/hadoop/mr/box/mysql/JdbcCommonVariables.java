package com.littlehotspot.hadoop.mr.box.mysql;

import com.littlehotspot.hadoop.mr.box.mysql.model.Model;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-30 上午 10:06.
 */
public class JdbcCommonVariables {

    public static final String DB_URL = "jdbc:mysql://rr-2zevja6lfg5718e3ko.mysql.rds.aliyuncs.com:3306/cloud?useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&zeroDateTimeBehavior=convertToNull";
//    public static final String DB_URL = "jdbc:mysql://192.168.2.145:3306/cloud?useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&zeroDateTimeBehavior=convertToNull";

    public static final String USER_NAME = "java_api_read";
//    public static final String USER_NAME = "phpweb";

    public static final String USER_PASSWD = "KESs23DRZVX7hrqe";
//    public static final String USER_PASSWD = "123456";

    public static final String DRIVER_TYPE="com.mysql.jdbc.Driver";

    /**
     * 查询结果
     */
    public static Map<String,String> modelMaps = new ConcurrentHashMap<>();

}
