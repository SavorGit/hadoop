package com.littlehotspot.hadoop.mr.hbase.room;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-07 下午 3:12.
 */
public class DBInputRoomMapper extends Mapper<LongWritable, RoomWritable, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, RoomWritable value, Context context) throws IOException, InterruptedException {
        try {
            if (value.getId() == null) {
                return;
            }
            byte[] rowKeyBytes = Bytes.toBytes(value.getId()+"");

            ImmutableBytesWritable rowKey;
            rowKey = new ImmutableBytesWritable(rowKeyBytes);

            context.write(rowKey, value.toPut());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
