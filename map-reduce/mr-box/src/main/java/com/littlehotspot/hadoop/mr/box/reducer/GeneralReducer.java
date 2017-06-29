/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile.reducer
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:51
 */
package com.littlehotspot.hadoop.mr.box.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <h1>Reducer - 通用</h1>
 * 输出文件的每一行内容为 MAPPER 的 KEY
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月25日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class GeneralReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * 通用 Reducer
     *
     * @param key     文本键
     * @param value   文本值列表
     * @param context Reducer 上下文
     * @throws IOException          输入输出异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        try {
            context.write(key, new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
