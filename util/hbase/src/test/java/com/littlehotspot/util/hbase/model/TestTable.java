/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.util.hbase.model
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:40
 */
package com.littlehotspot.util.hbase.model;

import com.littlehotspot.util.hbase.HBaseColumn;
import com.littlehotspot.util.hbase.HBaseFamily;
import com.littlehotspot.util.hbase.HBaseRowKey;
import com.littlehotspot.util.hbase.HBaseTable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年07月19日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@HBaseTable(name = "test_hbase")
@Data
@AllArgsConstructor
public class TestTable {

    @HBaseRowKey
    private String rowKey;

    @HBaseFamily(name = "basic")
    @HBaseColumn(name = "name")
    private String name;

    @HBaseFamily(name = "attr")
    private TestFamilyAttribute attribute;
}
