/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.cdf
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:15
 */
package com.littlehotspot.hadoop.mr.nginx.module.cdf;

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
    public static Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - [^ ]+ \\[(.+)\\] ([A-Z]+) ([^ ]+) HTTP/[^ ]+ \"(\\d{3})\" \\d+ \"(.+)\" \"([^-]+.*)\" \"(.+)\" \"(.+)\"$");

}
