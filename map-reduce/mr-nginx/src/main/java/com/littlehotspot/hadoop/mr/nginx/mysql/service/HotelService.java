package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IHotelMapper;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 2:56.
 */
public class HotelService {

    private static IHotelMapper hotelMapper = (IHotelMapper) Context.context.getBean("IHotelMapper");

    public int getCount() {

        System.out.println(hotelMapper);
        return hotelMapper.getCount();
    }
}
