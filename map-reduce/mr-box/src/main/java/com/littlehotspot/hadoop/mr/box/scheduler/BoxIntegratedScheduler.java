/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:31
 */
package com.littlehotspot.hadoop.mr.box.scheduler;

import com.littlehotspot.hadoop.mr.box.mysql.JdbcCommonVariables;
import com.littlehotspot.hadoop.mr.box.common.Argument;
import com.littlehotspot.hadoop.mr.box.mapper.BoxIntegratedMapper;
import com.littlehotspot.hadoop.mr.box.mysql.JDBCTool;
import com.littlehotspot.hadoop.mr.box.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.box.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.box.mysql.model.Media;
import com.littlehotspot.hadoop.mr.box.mysql.model.Model;
import com.littlehotspot.hadoop.mr.box.mysql.model.Room;
import com.littlehotspot.hadoop.mr.box.reducer.BoxIntegratedReducer;
import com.littlehotspot.hadoop.mr.box.util.Constant;
import com.littlehotspot.hadoop.mr.box.util.PathUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 调度器 - 机顶盒日志
 */
public class BoxIntegratedScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            // 获取参数
            String hdfsInputPath = Constant.CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = Constant.CommonVariables.getParameterValue(Argument.OutputPath);

            JDBCTool jdbcTool= JdbcReader.createSimpleJdbc();
            //查询hotel
            integQuery(jdbcTool,Hotel.class,"select id,name from savor_hotel");
            //查询room
            integQuery(jdbcTool,Room.class,"select id,name from savor_room");
            //查询media
            integQuery(jdbcTool,Media.class,"select id,name from savor_media");
            JdbcReader.closeJdbc();

            //封装数据
            for(Map.Entry<String,String> entry: JdbcCommonVariables.modelMaps.entrySet()){
                this.getConf().set(entry.getKey(),entry.getValue());
            }

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.setInputPaths(job, inputPath);
            job.setMapperClass(BoxIntegratedMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()),this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(BoxIntegratedReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }
            PathUtil.deletePath(this.getConf(),inputPath);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private void putToMap(List<Model> modelList){
        if(modelList==null || modelList.size()==0){
            return;
        }
        for(Model model:modelList){
            String key=model.getClass().getSimpleName()+model.getId();
            JdbcCommonVariables.modelMaps.put(key,model.getName());
        }
    }

    private  void integQuery(JDBCTool jdbcTool,Class<? extends Model> t,String sql,Object...params){
        try {
            List tList=jdbcTool.findResult(t,sql,params);
            putToMap(tList);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
