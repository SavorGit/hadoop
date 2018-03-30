/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 17:47
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import lombok.NoArgsConstructor;

/**
 * <h1>运算 - 日期</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@NoArgsConstructor
class DataOperatorForString implements IDataOperator {

    public boolean operate(OperationMode operationMode, String data) {
        try {
            boolean result;
            String dataString = data;
            String referenceString = operationMode.getValue();
            if (OperationSymbol.EQ == operationMode.getOperationSymbol()) {
                result = dataString.equals(referenceString);
            } else if (OperationSymbol.NE == operationMode.getOperationSymbol()) {
                result = !dataString.equals(referenceString);
            } else if (OperationSymbol.LT == operationMode.getOperationSymbol()) {
                result = dataString.compareTo(referenceString) < 0;
            } else if (OperationSymbol.LE == operationMode.getOperationSymbol()) {
                result = dataString.compareTo(referenceString) <= 0;
            } else if (OperationSymbol.GT == operationMode.getOperationSymbol()) {
                result = dataString.compareTo(referenceString) > 0;
            } else if (OperationSymbol.GE == operationMode.getOperationSymbol()) {
                result = dataString.compareTo(referenceString) >= 0;
            } else {
                result = false;
            }
            return result;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

//    public static void main(String[] args) {
//        String str = "20171205";
//        OperationMode operationMode = new OperationMode(0, OperationSymbol.LT, "20171207", null, String.class);
//        System.out.println(operation(operationMode, str));
//    }
}
