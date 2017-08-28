package com.littlehotspot.hadoop.mr.box.mysql;

import com.littlehotspot.hadoop.mr.box.common.CommonVariables;
import com.littlehotspot.hadoop.mr.box.mysql.model.SelectModel;
import com.littlehotspot.hadoop.mr.box.mapper.JdbcToHdfsMapper;
import com.littlehotspot.hadoop.mr.box.mapper.JdbcToMapMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
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
    /**
     * 读取mysql到map
     * @param hdfsCluster
     * @param selectModel
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void readToMap(String hdfsCluster, SelectModel selectModel) throws IOException, URISyntaxException {
        JobConf jobConf = new JobConf(JdbcReader.class);
        setJdbc(jobConf,hdfsCluster,selectModel);
        jobConf.setMapperClass(JdbcToMapMapper.class); // map
        jobConf.setReducerClass(IdentityReducer.class);
        JobClient.runJob(jobConf);
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

        DBConfiguration.configureDB(jobConf, JdbcCommonVariables.DRIVER_TYPE,
                JdbcCommonVariables.DB_URL, JdbcCommonVariables.USER_NAME, JdbcCommonVariables.USER_PASSWD);

        DBInputFormat.setInput(jobConf, selectModel.getInputClass(),selectModel.getQuery(),selectModel.getCountQuery());
    }


    public static JDBCTool createSimpleJdbc(){
        if(CommonVariables.jdbcTool!=null){
            return CommonVariables.jdbcTool;
        }
        JDBCTool jdbcTool=new JDBCTool(JdbcCommonVariables.DRIVER_TYPE,JdbcCommonVariables.DB_URL,JdbcCommonVariables.USER_NAME,JdbcCommonVariables.USER_PASSWD);
        jdbcTool.getConnection();
        CommonVariables.jdbcTool=jdbcTool;
        return  jdbcTool;
    }

    public static void closeJdbc(){
        if(CommonVariables.jdbcTool!=null){
            CommonVariables.jdbcTool.releaseConnection();
            CommonVariables.jdbcTool=null;
        }
    }

}


