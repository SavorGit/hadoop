package com.littlehotspot.hadoop.mr.nginx.util;

import org.apache.avro.data.Json;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 实体类和JSON对象之间相互转化
 * Created by Administrator on 2017-07-24 下午 7:39.
 */
public class JSONUtil {
    /**
     * 将json转化为实体POJO
     *
     * @param jsonStr
     * @param obj
     * @return
     */
    public static <T> Object JSONToObj(String jsonStr, Class<T> obj) {
        T t = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            t = objectMapper.readValue(jsonStr,
                    obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
    }

    /**
     * 将实体POJO转化为JSON
     *
     * @param obj
     * @return
     * @throws JSONException
     * @throws IOException
     */
    public static <T> JSONObject objectToJson(T obj) throws JSONException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        // Convert object to JSON string
        String jsonStr = "";
        try {
            jsonStr = mapper.writeValueAsString(obj);
        } catch (IOException e) {
            throw e;
        }
        return new JSONObject(jsonStr);
    }

    /**
     * 将jsonArray转化为List
     *
     * @param jsonStr
     * @param obj
     * @return
     */
    public static <T> List<Object> JSONArrayToList(String jsonStr, Class<T> obj) {

        List<Object> list = new ArrayList<>();

        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            for (int i = 0; i < jsonArray.length(); i++) {
                T t = (T) JSONToObj(jsonArray.get(i).toString(), obj);
                list.add(t);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * 将List转化为JSONArray
     *
     * @param obj
     * @return
     * @throws JSONException
     * @throws IOException
     */
    public static <T> JSONArray listToJsonArray(T obj) throws JSONException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        // Convert object to JSON string
        String jsonStr = "";
        try {
            jsonStr = mapper.writeValueAsString(obj);
        } catch (IOException e) {
            throw e;
        }
        return new JSONArray(jsonStr);
    }

}
