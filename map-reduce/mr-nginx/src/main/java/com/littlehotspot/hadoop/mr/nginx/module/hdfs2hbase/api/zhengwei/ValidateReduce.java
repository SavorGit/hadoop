package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.zhengwei;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorArea;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorHotel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorRoom;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * Created by gy on 2017/7/18.
 */
public class ValidateReduce extends Reducer<Text, Text, Text, Text> {

    private HBaseHelper hBaseHelper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            TargetBootRateBean bootBean = new TargetBootRateBean();
            TargetRareBean bean = new TargetRareBean();
            Integer count=0;
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                System.out.println(item.toString());
                String msg = item.toString();
                Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }
                Result medias=hBaseHelper.getOneRecord("medias", matcher.group(4));
                if (medias.isEmpty()){
                    System.out.println(matcher.group(4) + "RESULT IS EMPTY");
                    return;
                }
                    String type = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("type")));

                    String duration = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("duration")));
                    String name = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("name")));
                    bean.setPlayTime(duration);
                    bean.setHotelId(matcher.group(1));
                    bean.setRoomId(matcher.group(2));
                    bean.setDate(matcher.group(6));
                    bean.setTime(matcher.group(7));
                    bean.setOptionType(matcher.group(8));
                    bean.setMediaType(matcher.group(9));
                    bean.setContentName(name);
                    bean.setMac(matcher.group(3));
                    bean.setPlayDate(matcher.group(5));

            }

            bootBean.setRowKey(key.toString());
            bootBean.setTargetRareBean(bean);
            hBaseHelper.insert(bootBean);
//            context.write(new Text(bean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
