/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.util.hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:22
 */
package com.littlehotspot.util.hbase;

import java.lang.annotation.*;

/**
 * <h1>注解 - HBase 列族</h1>
 * 1、必须注解到最外层类的属性上
 * 2、必须注解到最外层类的方法上，此时必须配合注解 {@link HBaseColumn}
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年07月06日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@Documented
@Target({ElementType.FIELD, ElementType.METHOD})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseFamily {

    String name();
}
