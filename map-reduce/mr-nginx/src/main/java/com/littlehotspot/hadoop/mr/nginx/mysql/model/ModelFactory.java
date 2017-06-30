package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-30 下午 2:55.
 */
public class ModelFactory {

    /**
     *
     * @param model
     * @return
     */
    public static Model getModel(Model model){
        try {
            Map<String, Object> map = Obj2Map(model);

            Model model1 = (Model) map2Obj(map, Class.forName(model.getClass().getName()));

            return model1;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static Map<String,Object> Obj2Map(Object obj) throws Exception{
        Map<String,Object> map=new HashMap<String, Object>();
        Field[] fields = obj.getClass().getDeclaredFields();
        for(Field field:fields){
            field.setAccessible(true);
            map.put(field.getName(), field.get(obj));
        }
        return map;
    }

    public static Object map2Obj(Map<String,Object> map,Class<?> clz) throws Exception{
        Object obj = clz.newInstance();
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        for(Field field:declaredFields){
            int mod = field.getModifiers();
            if(Modifier.isStatic(mod) || Modifier.isFinal(mod)){
                continue;
            }
            field.setAccessible(true);
            field.set(obj, map.get(field.getName()));
        }
        return obj;
    }

}
