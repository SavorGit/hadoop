package com.littlehotspot.hadoop.web.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-22 下午 4:49.
 */
public class HBaseUtil {
    public static void addTableCoprocessor(String tableName, String coprocessorClassName) {
        String coprocessClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";


        try {

            HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());

//            admin.disableTable(tableName);
//
//            HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
//
//            htd.addCoprocessor(coprocessClassName);
//
//            admin.modifyTable(tableName, htd);
//
//            admin.enableTable(tableName);
        } catch (IOException e) {
//            logger.info(e.getMessage(), e);
        }
    }

    public static long rowCount(String tableName, String family) {

        Configuration conf = new Configuration();
        AggregationClient ac = new AggregationClient(conf);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        long rowCount = 0;
        try {
            rowCount = ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    public static long rowCount(String tableName) {

        Configuration conf = new Configuration();
        long rowCount = 0;
        try {
            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                rowCount += result.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

}
