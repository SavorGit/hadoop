package com.littlehotspot.hadoop.web.service.impl;

import com.littlehotspot.hadoop.web.service.ITestService;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
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
    public List<Map<String, Object>> getService(FilterList filterList, String startRow, int pageSize, int pageIndex, String tableName) {
//        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");

        Configuration conf = new Configuration();
        Scan scan = new Scan();

        PageFilter pageFilter;
        if (startRow == null) {// 如果不知道startRow，那么要查询0-pageSize * pageIndex条数据，结果按页取
            pageFilter = new PageFilter(pageSize * pageIndex);
        } else {//默认包含开始行，多查询一条，结果去掉最前面一行
            pageFilter = new PageFilter(pageSize + 1);
            scan.setStartRow(Bytes.toBytes(startRow));
        }

        //设置过滤器
        if (filterList != null) {
            filterList.addFilter(pageFilter);
            scan.setFilter(filterList);
        } else {
            scan.setFilter(pageFilter);
        }

        List<Map<String, Object>> list = new ArrayList<>();
        HTable hTable = null;
        try {
            hTable = new HTable(conf, tableName);
            ResultScanner ResultScannerFilterList = hTable.getScanner(scan);
            for (Result result : ResultScannerFilterList) {
                Map<String, Object> map = new HashedMap();
                for (KeyValue kv : result.raw()) {
                    map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
                }
                map.put("rowKey", Bytes.toString(result.getRow()));
                list.add(map);
            }
            ResultScannerFilterList.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 处理结果
        List<Map<String, Object>> resultList = new ArrayList<>();
        if (list.size() > 0) {
            if (startRow != null) {
                list.remove(0);
                resultList = list;
            } else {
                for (int i = pageSize * (pageIndex - 1); i < list.size(); i++) {
                    resultList.add(list.get(i));
                }
            }
        }

        return resultList;
    }
}
