package com.littlehotspot.hadoop.web.util;

import lombok.Data;

/**
 * <h1> 接口返回模型 </h1>
 * Created by Administrator on 2017-04-15 ${time}.
 */
@Data
public class StatusCode {

    private int code;

    private String message;

    private Object data;

    public StatusCode(int code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public StatusCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toString() {
//        return "{" +
//                "\"code\":" + code +
//                ", \"message\":" + "\"" + message + "\"" +
//                ", \"data\":" + data +
//                '}';
        return "StatusCode(code=" + this.getCode() + ", message=" + this.getMessage() + ", data=" + this.getData() + ")";

    }
}
