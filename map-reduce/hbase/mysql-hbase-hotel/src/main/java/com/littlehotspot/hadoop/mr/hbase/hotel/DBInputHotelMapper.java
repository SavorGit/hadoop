/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : demo
 * @Package : net.lizhaoweb.demo.hadoop.mysql2hdfs
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:49
 */
package com.littlehotspot.hadoop.mr.hbase.hotel;

import com.littlehotspot.hadoop.mr.hbase.io.HotelWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <h1>Mapper - 酒楼</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年08月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class DBInputHotelMapper extends Mapper<LongWritable, HotelWritable, ImmutableBytesWritable, Put> {

    @Override
    public void map(LongWritable key, HotelWritable value, Context context) throws IOException, InterruptedException {
        try {
            if (value.getId() == null) {
                return;
            }
            byte[] rowKeyBytes = Bytes.toBytes(value.getId());

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
            context.write(rowKey, value.toPut());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
