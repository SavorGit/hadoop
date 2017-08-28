package com.littlehotspot.hadoop.mr.box.hbase.mapper;

import com.littlehotspot.hadoop.mr.box.common.CommonVariables;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 *@Author 刘飞飞
 *@Date 2017/7/31 18:37
 */
public class BoxToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            Matcher matcher = CommonVariables.MAPPER_LOG_INTEGRATED_FORMAT_REGEX.matcher(line);
            if (!matcher.find()) {
                return;
            }
            long version = System.currentTimeMillis();
            String familyName = "attr";
            Long timestr=9999999999999L-Long.parseLong(matcher.group(4));
            String rowKeyStr=matcher.group(13)+"|"+matcher.group(6)+"|"+matcher.group(5)+"|"+timestr;
            Put put = new Put(Bytes.toBytes(rowKeyStr));// 设置rowkey

            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("uuid"), version, Bytes.toBytes(matcher.group(1)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_id"), version, Bytes.toBytes(matcher.group(2)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_id"), version, Bytes.toBytes(matcher.group(3)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("timestamps"), version, Bytes.toBytes(matcher.group(4)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("option_type"), version, Bytes.toBytes(matcher.group(5)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mda_type"), version, Bytes.toBytes(matcher.group(6)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mda_id"), version, Bytes.toBytes(matcher.group(7)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mobile_id"), version, Bytes.toBytes(matcher.group(8)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("apk_version"), version, Bytes.toBytes(matcher.group(9)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ads_period"), version, Bytes.toBytes(matcher.group(10)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("dmd_period"), version, Bytes.toBytes(matcher.group(11)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("com_value"), version, Bytes.toBytes(matcher.group(12)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mac"), version, Bytes.toBytes(matcher.group(13)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("date_time"), version, Bytes.toBytes(matcher.group(14)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_name"), version, Bytes.toBytes(matcher.group(15)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_name"), version, Bytes.toBytes(matcher.group(16)));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mda_name"), version, Bytes.toBytes(matcher.group(17)));

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(rowKeyStr));
            context.write(rowKey, put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
