package com.littlehotspot.hadoop.mr.box.mapper;

import com.littlehotspot.hadoop.mr.box.mysql.JdbcCommonVariables;
import com.littlehotspot.hadoop.mr.box.mysql.model.Model;
import com.littlehotspot.hadoop.mr.box.mysql.model.ModelFactory;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-04 下午 5:35.
 */
public class JdbcToMapMapper extends MapReduceBase implements Mapper<LongWritable, Model, LongWritable, Text> {
    @Override
    public void map(LongWritable longWritable, Model value, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {
        Model model = null;
        try {
            model = (Model) BeanUtils.cloneBean(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String key=model.getClass().getSimpleName()+"_"+model.getId();
        JdbcCommonVariables.modelMaps.put(key,model.getName());
    }
}