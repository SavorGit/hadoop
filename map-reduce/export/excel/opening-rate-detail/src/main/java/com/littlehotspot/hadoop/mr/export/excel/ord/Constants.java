/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:50
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import java.util.regex.Pattern;

/**
 * <h1>常量</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
class Constants {

    static class RegexPattern {
        static Pattern CUT_DATA = Pattern.compile("^$");
    }

    static class ConfigurationKey {
        static String EXCEL_WORKBOOK = ".excel.Workbook";
        static String REGEX_CUT_DATA = ConfigurationKey.class.getName() + ".regex.CUT_DATA";
        static String LIST_OPERATION_MODE = ConfigurationKey.class.getName() + ".list.OPERATION_MODE";
        static String CLASS_REGISTRAR_PREFIX_DATA_OPERATOR = "class.data.operator.";
        static String CLASS_REGISTRAR_PREFIX_FIELD = "class.field.";
    }
}
