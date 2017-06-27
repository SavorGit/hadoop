package com.littlehotspot.hadoop.mr.nginx.mysql.service;

import com.littlehotspot.hadoop.mr.nginx.mysql.Context;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IMediaMapper;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-27 下午 5:49.
 */
public class MediaService {
    private static IMediaMapper mediaMapper = (IMediaMapper) Context.context.getBean("IMediaMapper");

    public List<HashMap<String,Object>> getAll() {

        return mediaMapper.getAll();
    }

    public String getName(String id){
        if(StringUtils.isBlank(id)){
            return null;
        }

        String name = null;
        List<HashMap<String,Object>> medias = getAll();
        for (HashMap<String, Object> media : medias) {
            if(id.equals(media.get("id").toString())) {
                name = media.get("name").toString();
            }
        }
        return name;
    }
    
    public String getUrl(String id){
        if(StringUtils.isBlank(id)){
            return null;
        }

        String name = null;
        List<HashMap<String,Object>> medias = getAll();
        for (HashMap<String, Object> media : medias) {
            if(id.equals(media.get("id").toString())) {
                name = media.get("oss_addr").toString();
            }
        }
        return name;
    }
}
