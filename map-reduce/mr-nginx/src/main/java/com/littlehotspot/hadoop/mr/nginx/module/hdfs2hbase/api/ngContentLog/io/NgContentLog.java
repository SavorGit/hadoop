package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.io;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.CommonVariables;

import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-04 下午 9:07.
 */
public class NgContentLog {

    private String remote_addr; //ip

    private String remote_user; //

    private String time_local; //time

    private String request; //请求方式等

    private String status; // 状态

    private String body_bytes_sent; //

    private String http_referer; // 请求链接

    private String http_traceinfo; //

    private String http_user_agent; //

    private String http_x_forwarded_for; //

    public NgContentLog(String text) {
        Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(text);
        if (!matcher.find()) {
            return;
        }

        this.remote_addr = matcher.group(1);
        this.remote_user = matcher.group(2);
        this.time_local = matcher.group(3);
        this.request = matcher.group(4);
        this.status = matcher.group(5);
        this.body_bytes_sent = matcher.group(6);
        this.http_referer = matcher.group(7);
        this.http_traceinfo = matcher.group(8);
        this.http_user_agent = matcher.group(9);
        this.http_x_forwarded_for = matcher.group(10);
    }

    @Override
    public String toString() {
        return remote_addr + '|' +
                remote_user + '|' +
                time_local + '|' +
                request + '|' +
                status + '|' +
                body_bytes_sent + '|' +
                http_referer + '|' +
                http_traceinfo + '|' +
                http_user_agent + '|' +
                http_x_forwarded_for;
    }
}
