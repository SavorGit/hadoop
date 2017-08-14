package com.littlehotspot.hadoop.mr.hbase.hotelBoxIndex;

import com.littlehotspot.hadoop.mr.hbase.io.HotelBoxIndexWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-11 上午 10:25.
 */
public class DBInputHotelBoxIndexMapper extends Mapper<LongWritable, HotelBoxIndexWritable, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, HotelBoxIndexWritable value, Context context) throws IOException, InterruptedException {
        try {
            if (value.getHotel_id() == null) {
                return;
            }
            byte[] rowKeyBytes = Bytes.toBytes(value.getHotel_id() + "|" + value.getMac());

            ImmutableBytesWritable rowKey;
            rowKey = new ImmutableBytesWritable(rowKeyBytes);

            context.write(rowKey, value.toPut());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
