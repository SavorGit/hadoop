package com.littlehotspot.hadoop.mr.nginx.mysql.mapper;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-27 下午 5:48.
 */
public interface IMediaMapper {

    List<HashMap<String,Object>> getAll();
}
