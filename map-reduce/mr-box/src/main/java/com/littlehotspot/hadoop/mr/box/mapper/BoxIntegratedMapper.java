package com.littlehotspot.hadoop.mr.box.mapper;

import com.littlehotspot.hadoop.mr.box.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.box.common.CommonVariables;
import com.littlehotspot.hadoop.mr.box.mysql.model.Media;
import com.littlehotspot.hadoop.mr.box.mysql.model.Room;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * 整合数据mapper
 */
public class BoxIntegratedMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text ikey=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString().trim();
            Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }
            String hotelId = matcher.group(2); //酒楼Id
            String roomId=matcher.group(3); //包间Id
            String mediaId=matcher.group(7); //媒体Id
            String hotelName=context.getConfiguration().get(Hotel.class.getSimpleName()+hotelId,"");
            String roomName=context.getConfiguration().get(Room.class.getSimpleName()+roomId,"");
            String mediaName=context.getConfiguration().get(Media.class.getSimpleName()+mediaId,"");

            StringBuffer buffer=new StringBuffer(rowLineContent.trim());
            buffer.append(",");
            buffer.append(hotelName);
            buffer.append(",");
            buffer.append(roomName);
            buffer.append(",");
            buffer.append(mediaName);
            ikey.set(buffer.toString());
            context.write(ikey,NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
