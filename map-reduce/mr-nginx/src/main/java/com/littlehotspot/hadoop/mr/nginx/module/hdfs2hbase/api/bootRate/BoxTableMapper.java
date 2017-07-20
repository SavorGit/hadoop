package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by gy on 2017/7/18.
 */
public class BoxTableMapper extends TableMapper<Text, Text> {


    @Override
    protected void map(ImmutableBytesWritable rowKey, Result result, Context context){
//        System.out.println(result.toString());
        String row = Bytes.toString(result.getRow());
        String mediaId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_id")));
        String mediaType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_type")));
        String optionType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
        String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
        String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
        String hotelName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name")));
        String roomName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name")));
        String hotelId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_id")));
        String roomId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_id")));

        System.out.println("ROWKEY{"+row+"}"+":mda_type="+mediaType+":option_type="+optionType+":mda_id="+mediaId);
        try {
            if (StringUtils.isBlank(mediaId)){
                context.write(new Text(mac), new Text(mediaId));
            }else {
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
