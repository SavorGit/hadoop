package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.ICategoryMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IHotelMapper;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 2:56.
 */
public class CategoryService {

    private static ICategoryMapper categoryMapper = (ICategoryMapper) Context.context.getBean("ICategoryMapper");

    public List<HashMap<String,Object>> getAll() {

        return categoryMapper.getAll();
    }
}
