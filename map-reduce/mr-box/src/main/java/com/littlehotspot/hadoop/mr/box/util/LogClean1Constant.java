/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:36
 */
package com.littlehotspot.hadoop.mr.box.util;

import java.util.regex.Pattern;

/**
 * <h1>常量 - 机顶盒日志第一次清洗</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class LogClean1Constant extends Constant {

    /**
     * MAPPER 输入正则表达式
     */
    public static Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^.+,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.+,.*,?$");
}
