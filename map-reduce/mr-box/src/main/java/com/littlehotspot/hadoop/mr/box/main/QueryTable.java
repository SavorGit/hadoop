package com.littlehotspot.hadoop.mr.box.main;

import com.littlehotspot.hadoop.mr.box.util.Constant;
import org.apache.commons.lang.StringUtils;
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
    private static String regex="^2017\\d+\\|.+";
    private void query(String[] args) throws IOException {
        this.setConf(new Configuration());
        Constant.CommonVariables.initMapReduce(this.getConf(), args);
        Configuration conf = HBaseConfiguration.create(this.getConf());
        connection= ConnectionFactory.createConnection(conf);
        Table table=connection.getTable(TableName.valueOf("box_log"));
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("FCD5D900B607.+"));
//        SingleColumnValueFilter mda_id=new SingleColumnValueFilter(Bytes.toBytes("attr"),Bytes.toBytes("mobile_id"),CompareFilter.CompareOp.NOT_EQUAL,new RegexStringComparator("^\\s*$"));
        SingleColumnValueFilter date_time=new SingleColumnValueFilter(Bytes.toBytes("attr"),Bytes.toBytes("date_time"),CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^20170905.+$"));
//        Filter filter1=new PageFilter(MAX_RESULT_SIZE);
        SingleColumnValueFilter mda_id=new SingleColumnValueFilter(Bytes.toBytes("attr"),Bytes.toBytes("mda_id"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("7024"));
        Filter filterList=new FilterList(mda_id,date_time);

        Scan s = new Scan();
        s.setFilter(filterList);
//        s.setMaxResultSize(MAX_RESULT_SIZE);
        s.addColumn(Bytes.toBytes("attr"),Bytes.toBytes("uuid"));
        s.addColumn(Bytes.toBytes("attr"),Bytes.toBytes("mda_id"));
        s.addColumn(Bytes.toBytes("attr"),Bytes.toBytes("date_time"));
        ResultScanner rs = table.getScanner(s);
        String rowkey;
        String mda=null;
        String dateTime=null;
        int i=0;
        for(Result r:rs){
            i++;
            mda=Bytes.toString(r.getValue(Bytes.toBytes("attr"),Bytes.toBytes("mda_id")));
            dateTime=Bytes.toString(r.getValue(Bytes.toBytes("attr"),Bytes.toBytes("date_time")));
            rowkey=Bytes.toString(r.getRow());
//            if(!StringUtils.startsWith(mda,"201708")){
//               continue;
//            }
            if(i>0 && i%10000==0){
                System.out.println("当前总数整==="+i);
            }
            if(i>0){
                System.out.println(mda);
                System.out.println(rowkey);
                System.out.println(dateTime);
                System.out.println("当前总数==="+i);
            }
//
//            System.out.println(rowkey);
//            if(rowkey.matches(regex)){
//                deleteRecord(rowkey);
//                i++;
//                System.out.println(rowkey);
//                System.out.println("当前总数==="+i);
//            }else{
////                System.out.println("误差==="+rowkey);
//            }



        }
        System.out.println(i);
        connection.close();
    }

    private void  deleteRecord(String rowkey) throws IOException {
        Table table=this.connection.getTable(TableName.valueOf("box_log"));
        Delete del = new Delete(Bytes.toBytes(rowkey));
        table.delete(del);
    }

    public static void main(String[] args) throws IOException {
        args = new String[4];
//        args[0]="hdfsCluster=hdfs://devpd1:8020";
        args[0]="hdfsCluster=hdfs://onlinemain:8020";
//        args[1]="hbaseRoot=hdfs://devpd1:8020/hbase";
        args[1]="hbaseRoot=hdfs://onlinemain:8020/hbase";
//        args[2]="hbaseZookeeper=devpd1";
        args[2]="hbaseZookeeper=onlinemain";
//        args[3]="hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase";
        args[3]="hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase";
        QueryTable queryTable=new QueryTable();
        queryTable.query(args);
    }
}
