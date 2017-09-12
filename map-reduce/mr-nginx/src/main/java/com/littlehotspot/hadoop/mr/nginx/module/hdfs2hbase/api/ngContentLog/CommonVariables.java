package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-24 下午 3:34.
 */
public class CommonVariables extends Constant.CommonVariables {

    // 日志匹配
    public static Pattern MAPPER_LOG_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - (.*) \\[(.*)\\] (.*) \"(.*)\" (.*) \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\"$");

    // 清洗后日志匹配
    static Pattern MAPPER_HDFS_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3})\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$");

    // 链接匹配
    static Pattern MAPPER_HTTP_REQUEST = Pattern.compile("\\s\\/content\\/(\\d{1,})\\.html\\?app=[^\\s\\?]*&(channel=[^\\s\\?\\\\]*)(\\\\x.*)?\\s.*");

    static Pattern MAPPER_WX = Pattern.compile("MicroMessenger\\/");
    static Pattern MAPPER_NET_TYPE = Pattern.compile("NetType\\/([a-zA-Z\\d]*) ");
    static Pattern MAPPER_DEVICE_iPhone = Pattern.compile("CPU iPhone OS \\d{1,}(_\\d{1,}){0,}");
    static Pattern MAPPER_DEVICE_Android = Pattern.compile("Android \\d{1,}(\\.\\d{1,}){0,};");
    static Pattern MAPPER_DEVICE_MAC = Pattern.compile("Intel Mac OS X \\d{1,}(_\\d{1,}){0,}");
    static Pattern MAPPER_DEVICE_WINDOWS = Pattern.compile("Windows NT \\d{1,}(\\.\\d{1,}){0,};");

    static SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

    static String httpUrl1 = "/content/2692.html?app=inner&channel=bigscpro";
    static String httpUrl2 = "/content/3340.html?app=inner&channel=bigscpro";

    public static final String HBASE_TABLE_NAME = "ng_content_log";

    static final Pattern MYSQL_ROW_PATTERN = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3})\u0001(.*)\u0001(.*)\u0001(.*)\u0001([1-9]\\d*)\u0001(\\d{1,})\u0001(.*)\u0001(.*)\u0001(.*)$");

}