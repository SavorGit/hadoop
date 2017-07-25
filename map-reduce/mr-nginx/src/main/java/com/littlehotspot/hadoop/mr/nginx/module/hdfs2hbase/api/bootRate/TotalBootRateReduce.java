package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

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
public class TotalBootRateReduce extends Reducer<Text, Text, Text, Text> {

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
            TotalTargetRareBean bean = new TotalTargetRareBean();

            Integer count=0;
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    System.out.println(item.toString());
                    String msg = item.toString();
                    Matcher matcher = CommonVariables.MAPPER_RATE_LOG_FORMAT_REGEX.matcher(msg);
                    if (!matcher.find()){
                        return;
                    }
                if (!StringUtils.isBlank(matcher.group(11))){
                    if (!StringUtils.isBlank(bean.getProduction())){
                        Double v = Double.valueOf(bean.getProduction()) + Double.valueOf(matcher.group(11));
                        bean.setProduction(v.toString());
                    }else {
                        bean.setProduction(matcher.group(11));
                    }
                }


                count++;

            }
            Double v = Double.valueOf(bean.getProduction()) / count;
            bean.setAvProduction(v.toString());
            bean.setPlayCount(count.toString());

//            hBaseHelper.insert(bootBean);
//            context.write(new Text(bean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
