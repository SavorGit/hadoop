package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gy on 2017/7/18.
 */
public class BoxTableReduce extends Reducer<Text, Text, Text, Text> {

    private HBaseHelper hBaseHelper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                System.out.println(item.toString());
                if (StringUtils.isBlank(item.toString())){

                    Result medias = hBaseHelper.getOneRecord("medias", item.toString());
                    String duration = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("duration")));


                    System.out.println(medias.toString());
                }


            }


            context.write(key, new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
