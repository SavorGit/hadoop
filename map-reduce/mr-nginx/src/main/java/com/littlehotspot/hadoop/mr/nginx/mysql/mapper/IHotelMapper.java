package com.littlehotspot.hadoop.mr.nginx.mysql.mapper;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 3:00.
 */
public interface IHotelMapper {
    int getCount();

    List<HashMap<String,Object>> getAll();
}