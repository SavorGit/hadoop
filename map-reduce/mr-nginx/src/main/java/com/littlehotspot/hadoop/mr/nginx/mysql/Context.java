package com.littlehotspot.hadoop.mr.nginx.mysql;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-23 下午 4:26.
 */
public class Context {
    
    public static ApplicationContext context=new ClassPathXmlApplicationContext("spring/spring-mysql-mapper.xml");
    
}
