package com.littlehotspot.hadoop.web.service;

import org.apache.hadoop.hbase.filter.FilterList;

import java.util.List;
import java.util.Map;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-17 下午 3:04.
 */
public interface ITestService {
    public List<Map<String,Object>> getService(FilterList filterList);
}
