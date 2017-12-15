package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.small.download;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;

import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-24 下午 3:34.
 */
public class CommonVariables extends Constant.CommonVariables {

    public static Pattern MAPPER_LOG_FORMAT_REGEX = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

}
