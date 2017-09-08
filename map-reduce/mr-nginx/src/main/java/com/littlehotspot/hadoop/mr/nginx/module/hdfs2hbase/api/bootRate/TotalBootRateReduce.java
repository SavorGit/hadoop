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

    private String issue;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
        issue = conf.get("issue");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            TotalTargetRareBean bean = new TotalTargetRareBean();
            TargetTotalBootRateBean rateBean = new TargetTotalBootRateBean();
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
                    String production = matcher.group(11).substring(0,matcher.group(11).length()-1);
                if (!StringUtils.isBlank(production)){
                    if (!StringUtils.isBlank(bean.getProduction())){
                        Double v = Double.valueOf(bean.getProduction()) + Double.valueOf(production);
                        String s1 = String.format("%.2f", v);
                        bean.setProduction(s1);
                    }else {
                        String s1 = String.format("%.2f", Double.valueOf(production));
                        bean.setProduction(s1);
                    }
                }
                bean.setArea(matcher.group(1));
                bean.setHotelName(matcher.group(2));
                bean.setAddr(matcher.group(3));
                bean.setRoomName(matcher.group(4));
                bean.setMaintenMan(matcher.group(5));
                bean.setIsKey(matcher.group(6));
                bean.setMac(matcher.group(8));
                count++;

            }
            Double v = Double.valueOf(bean.getProduction()) / count;
            String s1 = String.format("%.2f", v);
            bean.setAvProduction(s1);
            bean.setPlayCount(count.toString());
            bean.setIssue(issue);
            rateBean.setRowKey(key.toString()+issue);
            rateBean.setTotalTargetRareBean(bean);
            if (Integer.parseInt(bean.getPlayCount())>=7){
                if (bean.getAddr().equals("大厅")){
                    if ((Double.valueOf(bean.getAvProduction())-0.8)>=0.00){
                        hBaseHelper.insert(rateBean);
                    }

                }else if (bean.getAddr().equals("包间")){
                    if (Double.valueOf(bean.getAvProduction())>=0.40){
                        hBaseHelper.insert(rateBean);
                    }
                }else if (bean.getAddr().equals("等候区")){
                    if ((Double.valueOf(bean.getAvProduction())-0.8)>=0.00){
                        hBaseHelper.insert(rateBean);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
