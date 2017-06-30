package com.littlehotspot.hadoop.mr.nginx.mysql;

import com.littlehotspot.hadoop.mr.nginx.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.Model;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.ModelFactory;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <h1> mysql读取类 </h1>
 * Created by Administrator on 2017-06-29 下午 6:03.
 */
public class JdbcReader {

    public static void main(String[] args) throws IOException, URISyntaxException {

        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Hotel.class);
        selectModel.setTableName("savor_hotel");
        selectModel.setFields(MysqlCommonVariables.hotelFields);
        selectModel.setOutput("/home/data/hadoop/flume/test_hbase/mysql");

        read("hdfs://devpd1:8020",selectModel);

        System.out.println(MysqlCommonVariables.modelMap);
    }

    /**
     * 读取mysql
     * @param hdfsCluster
     * @param selectModel
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void read(String hdfsCluster, SelectModel selectModel) throws IOException, URISyntaxException {

        JobConf jobConf = new JobConf(JdbcReader.class);
        if(StringUtils.isNotBlank(hdfsCluster)) {
            jobConf.set("fs.defaultFS", hdfsCluster);
        }
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setInputFormat(DBInputFormat.class);

        Path outputPath = new Path(selectModel.getOutput());
        FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), jobConf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(jobConf, outputPath);

        DBConfiguration.configureDB(jobConf, "com.mysql.jdbc.Driver",
                MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);

        DBInputFormat.setInput(jobConf, selectModel.getInputClass(),
                selectModel.getTableName(), selectModel.getConditions(),
                selectModel.getOrderBy(), selectModel.getFields());

        jobConf.setMapperClass(JdbcMapper.class);
        jobConf.setReducerClass(IdentityReducer.class);
        JobClient.runJob(jobConf);

//        System.out.println(MysqlCommonVariables.modelMap);
    }

    static class JdbcMapper extends MapReduceBase implements Mapper<LongWritable, Model, LongWritable, Text> {

        @Override
        public void map(LongWritable longWritable, Model value, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {

            Model model = ModelFactory.getModel(value);

            MysqlCommonVariables.modelMap.put(model.getId(), model);

//            collector.collect(new LongWritable(value.getId()), new Text(value
//                    .toString()));
        }
    }

}


