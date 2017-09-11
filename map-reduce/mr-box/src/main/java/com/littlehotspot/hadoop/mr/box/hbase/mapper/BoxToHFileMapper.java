package com.littlehotspot.hadoop.mr.box.hbase.mapper;

import com.littlehotspot.hadoop.mr.box.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.box.mysql.model.Media;
import com.littlehotspot.hadoop.mr.box.mysql.model.Room;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *@Author 刘飞飞
 *@Date 2017/7/31 18:37
 */
public class BoxToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
    ImmutableBytesWritable rowKey=new ImmutableBytesWritable();
    public static String SPLIT=",";
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            String[] vals= StringUtils.splitByWholeSeparatorPreserveAllTokens(line,SPLIT);
//            Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(line);
            if (vals.length<14 || vals.length>15) {
                return;
            }
            long version = System.currentTimeMillis();
            String hotelId = vals[1]; //酒楼Id
            String roomId=vals[2]; //包间Id
            String mediaId=vals[6]; //媒体Id
            String hotelName=context.getConfiguration().get(Hotel.class.getSimpleName()+hotelId,"");
            String roomName=context.getConfiguration().get(Room.class.getSimpleName()+roomId,"");
            String mediaName=context.getConfiguration().get(Media.class.getSimpleName()+mediaId,"");
            String familyName = "attr";
            Long timestr=9999999999999L-Long.parseLong(vals[3]);
            String rowKeyStr=vals[12]+"|"+vals[5]+"|"+vals[4]+"|"+timestr;
            Put put = new Put(Bytes.toBytes(rowKeyStr));// 设置rowkey

            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("uuid"), version, Bytes.toBytes(vals[0]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_id"), version, Bytes.toBytes(vals[1]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_id"), version, Bytes.toBytes(vals[2]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("timestamps"), version, Bytes.toBytes(vals[3]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("option_type"), version, Bytes.toBytes(vals[4]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mda_type"), version, Bytes.toBytes(vals[5]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mda_id"), version, Bytes.toBytes(vals[6]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mobile_id"), version, Bytes.toBytes(vals[7]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("apk_version"), version, Bytes.toBytes(vals[8]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ads_period"), version, Bytes.toBytes(vals[9]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("dmd_period"), version, Bytes.toBytes(vals[10]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("com_value"), version, Bytes.toBytes(vals[11]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mac"), version, Bytes.toBytes(vals[12]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("date_time"), version, Bytes.toBytes(vals[13]));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_name"), version, Bytes.toBytes(hotelName));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_name"), version, Bytes.toBytes(roomName));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mda_name"), version, Bytes.toBytes(mediaName));

//            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(rowKeyStr));
            rowKey.set(Bytes.toBytes(rowKeyStr));
            context.write(rowKey, put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
