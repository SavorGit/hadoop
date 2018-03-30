/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 17:45
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * <h1>模型 - 运算符号</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
enum OperationSymbol {
    EQ("=="),
    NE("!="),
    GT(">"),
    LT("<"),
    GE(">="),
    LE("<="),;

    @NonNull
    private String name;

    public static OperationSymbol fromName(String name) {
        for (OperationSymbol operationSymbol : values()) {
            if (operationSymbol.name.equals(name)) {
                return operationSymbol;
            }
        }
        return null;
    }
}
