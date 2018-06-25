/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.hive
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:52
 */
package com.littlehotspot.hadoop.mr.export.excel.hive;

import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;

/**
 * <h1>枚举 - 参数</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月25日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ExcelArgument extends Argument {

    public static final Argument ReduceInPatternValue = new Argument("reduceInRegexValue", null, null);// JDBC 驱动

    public ExcelArgument(String name, String nullValue, String[] nullArray) {
        super(name, nullValue, nullArray);
    }
}
