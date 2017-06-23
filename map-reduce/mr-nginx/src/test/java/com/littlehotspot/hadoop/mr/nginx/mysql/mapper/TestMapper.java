package com.littlehotspot.hadoop.mr.nginx.mysql.mapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

    @Test
    public void getCount(){
        System.out.println(hotelMapper);
        System.out.println(hotelMapper.getCount());
    }
}
