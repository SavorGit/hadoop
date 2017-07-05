package com.littlehotspot.hadoop.mr.nginx.mysql;

import com.littlehotspot.hadoop.mr.nginx.mysql.model.Hotel;
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
import java.util.HashMap;

/**
 * <h1> mysql读取类 </h1>
 * Created by Administrator on 2017-06-29 下午 6:03.
 */
public class JdbcReader {

    public static void main(String[] args) throws IOException, URISyntaxException {

        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Hotel.class);
        selectModel.setQuery("select id,name from savor_hotel");
        selectModel.setCountQuery("select count(*) from savor_hotel");
        selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

        readToMap("hdfs://devpd1:8020",selectModel);

        System.out.println(MysqlCommonVariables.modelMap);
    }

    /**
     * 读取mysql到map
     * @param hdfsCluster
     * @param selectModel
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void readToMap(String hdfsCluster, SelectModel selectModel) throws IOException, URISyntaxException {
        MysqlCommonVariables.modelMap = new HashMap<>();

        JobConf jobConf = new JobConf(JdbcReader.class);
        setJdbc(jobConf,hdfsCluster,selectModel);

        jobConf.setMapperClass(JdbcToMapMapper.class); // map
        jobConf.setReducerClass(IdentityReducer.class);
        JobClient.runJob(jobConf);

//        System.out.println(MysqlCommonVariables.modelMap);
    }


    /**
     * 读取mysql到hdfs
     * @param hdfsCluster
     * @param selectModel
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void readToHdfs(String hdfsCluster, SelectModel selectModel) throws IOException, URISyntaxException {

        JobConf jobConf = new JobConf(JdbcReader.class);
        setJdbc(jobConf,hdfsCluster,selectModel);

        jobConf.setMapperClass(JdbcToHdfsMapper.class); // hdfs
        jobConf.setReducerClass(IdentityReducer.class);
        JobClient.runJob(jobConf);

    }

    /**
     * setjdbc各种属性
     * @param jobConf
     * @param hdfsCluster
     * @param selectModel
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void setJdbc(JobConf jobConf, String hdfsCluster, SelectModel selectModel) throws IOException, URISyntaxException{
        if(StringUtils.isNotBlank(hdfsCluster)) {
            jobConf.set("fs.defaultFS", hdfsCluster);
        }
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setInputFormat(DBInputFormat.class);

        Path outputPath = new Path(selectModel.getOutputPath());
        FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), jobConf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(jobConf, outputPath);

        DBConfiguration.configureDB(jobConf, "com.mysql.jdbc.Driver",
                MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);

        DBInputFormat.setInput(jobConf, selectModel.getInputClass(),selectModel.getQuery(),selectModel.getCountQuery());
    }

}


