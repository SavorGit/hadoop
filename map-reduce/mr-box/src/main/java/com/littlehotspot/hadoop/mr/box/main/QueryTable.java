package com.littlehotspot.hadoop.mr.box.main;

import com.littlehotspot.hadoop.mr.box.util.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 *@Author 刘飞飞
 *@Date 2017/7/24 10:10
 */
public class QueryTable extends Configured {
    private static int MAX_RESULT_SIZE=1;
    private static Connection connection;
    private static String regex="[^\\|]+";
    private void query(String[] args) throws IOException {
        this.setConf(new Configuration());
        Constant.CommonVariables.initMapReduce(this.getConf(), args);
        Configuration conf = HBaseConfiguration.create(this.getConf());
        connection= ConnectionFactory.createConnection(conf);

        Table table=connection.getTable(TableName.valueOf("box_log"));
        Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,new RegexStringComparator(".+\\|.+"));
        Filter filter1=new PageFilter(MAX_RESULT_SIZE);
        Filter filterList=new FilterList(filter,filter1);

        Scan s = new Scan();
        s.setFilter(filter1);
        s.setMaxResultSize(MAX_RESULT_SIZE);
        s.addColumn(Bytes.toBytes("attr"),Bytes.toBytes("uuid"));
        ResultScanner rs = table.getScanner(s);
        String rowkey;
        for(Result r:rs){
            rowkey=Bytes.toString(r.getRow());
            System.out.println("fffff"+rowkey);
            if(rowkey.matches(regex)){
//                deleteRecord(rowkey);
                System.out.println(rowkey);
            }

        }
        connection.close();
    }

    private void  deleteRecord(String rowkey) throws IOException {
        Table table=this.connection.getTable(TableName.valueOf("box_log"));
        Delete del = new Delete(Bytes.toBytes(rowkey));
        table.delete(del);
    }

    public static void main(String[] args) throws IOException {
        args = new String[4];
        args[0]="hdfsCluster=hdfs://devpd1:8020";
        args[1]="hbaseRoot=hdfs://devpd1:8020/hbase";
        args[2]="hbaseZookeeper=devpd1";
        args[3]="hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase";
        QueryTable queryTable=new QueryTable();
        queryTable.query(args);
    }
}
