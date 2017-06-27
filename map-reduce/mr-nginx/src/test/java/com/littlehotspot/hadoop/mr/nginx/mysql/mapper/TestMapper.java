package com.littlehotspot.hadoop.mr.nginx.mysql.mapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.parsing.SourceExtractor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 3:11.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
        "classpath*:application.xml"
})
public class TestMapper {
    @Autowired
    private IHotelMapper hotelMapper;
    @Autowired
    private IRoomMapper roomMapper;
    @Autowired
    private IContentMapper contentMapper;
    @Autowired
    private ICategoryMapper categoryMapper;

    @Test
    public void getCount(){
//        System.out.println(hotelMapper);
//        System.out.println(hotelMapper.getCount());
//        List<HashMap<String, String>> all = hotelMapper.getAll();
//        for (HashMap<String, String> stringStringHashMap : all) {
//            System.out.println(stringStringHashMap.toString());
//        }

//        List<HashMap<String, String>> all = roomMapper.getAll();
//        for (HashMap<String, String> stringStringHashMap : all) {
//            System.out.println(stringStringHashMap.toString());
//        }
        List<HashMap<String, String>> all = contentMapper.getAll();
        for (HashMap<String, String> stringStringHashMap : all) {
            System.out.println(stringStringHashMap);
        }
        List<HashMap<String, String>> all1 = categoryMapper.getAll();
        for (HashMap<String, String> stringStringHashMap : all1) {
            System.out.println(stringStringHashMap);
        }
    }
}
