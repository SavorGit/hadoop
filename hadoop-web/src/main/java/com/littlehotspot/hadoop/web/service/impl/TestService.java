package com.littlehotspot.hadoop.web.service.impl;

import com.littlehotspot.hadoop.web.service.ITestService;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-17 下午 3:04.
 */
@Service
public class TestService implements ITestService {
    @Override
    public List<Map<String,Object>> getService(FilterList filterList) {
//        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");

        Configuration conf = new Configuration();
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);

        List<Map<String,Object>> list = new ArrayList<>();
        try {
            HTable hTable = new HTable(conf, "user_date");
            ResultScanner ResultScannerFilterList = hTable.getScanner(scan);
            for (Result result : ResultScannerFilterList) {
                Map<String,Object> map = new HashedMap();
                for (KeyValue kv : result.raw()) {
                    map.put(Bytes.toString(kv.getQualifier()),Bytes.toString(kv.getValue()));
                    System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s",
                            Bytes.toString(kv.getRow()),
                            Bytes.toString(kv.getFamily()),
                            Bytes.toString(kv.getQualifier()),
                            Bytes.toString(kv.getValue())));
                }
                list.add(map);
//                String dc = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("device_id")));
//                System.out.println(dc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }
}
