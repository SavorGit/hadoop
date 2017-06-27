package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IHotelMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IRoomMapper;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 2:56.
 */
public class RoomService {

    private static IRoomMapper roomMapper = (IRoomMapper) Context.context.getBean("IRoomMapper");

    public List<HashMap<String,Object>> getAll() {

        return roomMapper.getAll();
    }

    public String getName(String roomId){
        if(StringUtils.isBlank(roomId)){
            return null;
        }

        String name = null;
        List<HashMap<String,Object>> rooms = getAll();
        for (HashMap<String, Object> room : rooms) {
            if(roomId.equals(room.get("id").toString())) {
                name = room.get("name").toString();
            }
        }
        return name;
    }
}
