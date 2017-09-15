package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.boxTimeIndex;

import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-13 下午 2:18.
 */
public class Constant {
    public static final Pattern ROWKEY_PATTERN = Pattern.compile("([A-Z,a-z,0-9]+)\\|(.*)\\|(.*)\\|([1-9]\\d*)");
}
