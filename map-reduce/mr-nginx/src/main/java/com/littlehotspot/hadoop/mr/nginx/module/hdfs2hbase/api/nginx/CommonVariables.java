/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:25
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.nginx;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;

import java.util.regex.Pattern;

/**
 * <h1>公共变量 - 数据格式转换</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class CommonVariables extends Constant.CommonVariables {

    /**
     * Mapper 输入时正则过滤
     */
    public static Pattern MAPPER_NGINX_FORMAT_REGEX = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

//    public static Pattern MAPPER_NGINX_LOG_FORMAT_REGEX = Pattern.compile("^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$");
    public static Pattern MAPPER_NGINX_LOG_FORMAT_REGEX = Pattern.compile("^(.*)\\|(\\d*)\\|([A-Za-z]*)\\|(.*)\\|(\\d*)\\|(.*)\\|([A-Za-z0-9.-_]*)\\|(\\d*)\\|([A-Za-z0-9.-_]*)\\|([A-Za-z0-9.-_]*)\\|(.*)\\|([A-Za-z0-9.-_]*)\\|([A-Za-z0-9.-]*)\\|([A-Za-z0-9.-_]*)\\|(.*)\\|([A-Za-z0-9.-_]*)\\|(\\d*)\\|([A-Za-z0-9.-_]*)\\|(.*)\\|(.*)\\|(.*)s*$");

    public static Pattern MAPPER_BOX_FORMAT_REGEX = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_BOX_LOG_FORMAT_REGEX = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    public static Pattern MAPPER_MOBILE_FORMAT_REGEX = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_MOBILE_LOG_FORMAT_REGEX = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    public static HBaseHelper hBaseHelper;
}
