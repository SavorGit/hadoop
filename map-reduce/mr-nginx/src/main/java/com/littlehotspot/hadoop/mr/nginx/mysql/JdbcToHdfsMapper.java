package com.littlehotspot.hadoop.mr.nginx.mysql;

import com.littlehotspot.hadoop.mr.nginx.mysql.model.Model;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-04 下午 5:36.
 */
public class JdbcToHdfsMapper extends MapReduceBase implements Mapper<LongWritable, Model, LongWritable, Text> {

    @Override
    public void map(LongWritable longWritable, Model value, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {

            collector.collect(new LongWritable(Long.valueOf(value.getId())), new Text(value
                    .toString()));
    }
}
