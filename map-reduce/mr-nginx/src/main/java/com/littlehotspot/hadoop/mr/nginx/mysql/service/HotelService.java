package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IHotelMapper;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 2:56.
 */
public class HotelService {

    private static IHotelMapper hotelMapper = (IHotelMapper) Context.context.getBean("IHotelMapper");

    public int getCount() {

        return hotelMapper.getCount();
    }

    public List<HashMap<String,Object>> getAll() {

        return hotelMapper.getAll();
    }

    public String getName(String hotelId){
        if(StringUtils.isBlank(hotelId)){
            return null;
        }

        String name = null;
        List<HashMap<String,Object>> hotels = getAll();
        for (HashMap<String, Object> hotel : hotels) {
            if(hotelId.equals(hotel.get("id").toString())) {
                name = hotel.get("name").toString();
            }
        }
        return name;
    }
}
