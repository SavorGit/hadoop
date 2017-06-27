package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IBoxMapper;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-27 下午 5:42.
 */
public class BoxService {
    private static IBoxMapper boxMapper = (IBoxMapper) Context.context.getBean("IBoxMapper");

    public List<HashMap<String,Object>> getAll() {

        return boxMapper.getAll();
    }

    public String getName(String mac){
        if(StringUtils.isBlank(mac)){
            return null;
        }

        String name = null;
        List<HashMap<String,Object>> boxes = getAll();
        for (HashMap<String, Object> box : boxes) {
            if(mac.equals(box.get("mac").toString())) {
                name = box.get("name").toString();
            }
        }
        return name;
    }


}
