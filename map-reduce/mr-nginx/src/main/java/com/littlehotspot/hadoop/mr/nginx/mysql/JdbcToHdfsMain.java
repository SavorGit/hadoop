package com.littlehotspot.hadoop.mr.nginx.mysql;

import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.HdfsStringModel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;

import java.io.IOException;

/**
 * <h1> 读取mysql数据到hdfs </h1>
 * Created by Administrator on 2017-07-07 下午 6:00.
 */
public class JdbcToHdfsMain {

    public static void main(String[] args) throws IOException {
        String hdfsCluster;
        String outputPath;
        String sql;
        if (args != null && args.length > 2) {
            hdfsCluster = args[0];
            outputPath = args[1];
            sql = args[2];
        } else {
            throw new IOException("please write output path and sql...");
        }

        try {
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(HdfsStringModel.class);

            selectModel.setQuery(sql);
            selectModel.setCountQuery("select count(*) from savor_mb_content");

            selectModel.setOutputPath(outputPath);

            JdbcReader.readToHdfs(hdfsCluster, selectModel);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
