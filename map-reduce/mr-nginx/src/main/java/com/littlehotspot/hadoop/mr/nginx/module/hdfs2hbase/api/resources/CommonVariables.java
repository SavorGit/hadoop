package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;

import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 3:02.
 */
public class CommonVariables extends Constant.CommonVariables {
    /**
     * input regex
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static HBaseHelper hBaseHelper;
}
