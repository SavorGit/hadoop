package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IContentMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IHotelMapper;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 2:56.
 */
public class ContentService {

    private static IContentMapper contentMapper = (IContentMapper) Context.context.getBean("IContentMapper");

    public List<HashMap<String,Object>> getAll() {

        return contentMapper.getAll();
    }
}
