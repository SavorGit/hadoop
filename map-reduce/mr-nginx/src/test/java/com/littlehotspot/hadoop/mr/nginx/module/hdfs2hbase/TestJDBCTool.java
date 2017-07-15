/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:41
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase;

import com.littlehotspot.hadoop.mr.nginx.bean.HotelBean;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年07月14日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestJDBCTool {

    private String username;
    private String password;
    private String driver;
    private String url;

    @Before
    public void init() {
        this.driver = "com.mysql.jdbc.Driver";
        this.url = "jdbc:mysql://rr-2zevja6lfg5718e3ko.mysql.rds.aliyuncs.com:3306/cloud?useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&zeroDateTimeBehavior=convertToNull";
        this.username = "java_api_read";
        this.password = "KESs23DRZVX7hrqe";
    }

    @Test
    public void findResult() {
        String sql = "select id,name from savor_hotel";

        JDBCTool jdbcUtil = new JDBCTool(this.driver, this.url, this.username, this.password);
        jdbcUtil.getConnection();
        try {
            List<Map<String, Object>> result = jdbcUtil.findResult(sql);
            for (Map<String, Object> map : result) {
                System.out.println(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            jdbcUtil.releaseConnection();
        }
    }

    @Test
    public void findResultClass() {
        String sql = "select id,name from savor_hotel";

        JDBCTool jdbcUtil = new JDBCTool(this.driver, this.url, this.username, this.password);
        jdbcUtil.getConnection();
        try {
            List<HotelBean> result = jdbcUtil.findResult(HotelBean.class, sql);
            for (HotelBean bean : result) {
                System.out.println(bean);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            jdbcUtil.releaseConnection();
        }
    }
}
