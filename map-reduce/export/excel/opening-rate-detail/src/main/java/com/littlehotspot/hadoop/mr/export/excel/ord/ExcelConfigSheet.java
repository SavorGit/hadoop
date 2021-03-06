/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:22
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import lombok.Data;

import java.util.List;

/**
 * <h1>配置 - Excel Sheet</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@Data
public class ExcelConfigSheet {

    private int index;
    private String hdfsDir;
    private String fieldRegexSeparator;
    private String name;
    private String titles;
    private String titleRegexSeparator;

    private List<OperationMode> operationModeList;
}
