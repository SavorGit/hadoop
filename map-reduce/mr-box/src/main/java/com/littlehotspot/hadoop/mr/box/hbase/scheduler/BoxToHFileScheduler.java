package com.littlehotspot.hadoop.mr.box.hbase.scheduler;

import com.littlehotspot.hadoop.mr.box.common.Argument;
import com.littlehotspot.hadoop.mr.box.hbase.mapper.BoxToHFileMapper;
import com.littlehotspot.hadoop.mr.box.mysql.JDBCTool;
import com.littlehotspot.hadoop.mr.box.mysql.JdbcCommonVariables;
import com.littlehotspot.hadoop.mr.box.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.box.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.box.mysql.model.Media;
import com.littlehotspot.hadoop.mr.box.mysql.model.Model;
import com.littlehotspot.hadoop.mr.box.mysql.model.Room;
import com.littlehotspot.hadoop.mr.box.util.Constant;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 *@Author 刘飞飞
 *@Date 2017/7/31 18:52
 */
public class BoxToHFileScheduler extends Configured implements Tool {

    private String hTableName = "box_log";
    @Override
    public int run(String[] args) throws Exception {
        try {
            Constant.CommonVariables.initMapReduce(this.getConf(), args);
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

            Path outputPath = new Path(hdfsOutputPath);
            HTable hTable = new HTable(this.getConf(), this.hTableName);

            // 如果输出路径已经存在，则删除
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());
            job.setMapperClass(BoxToHFileMapper.class);
            job.setReducerClass(KeyValueSortReducer.class);

            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, hTable.getRegionLocator());
            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }
            // 导入到 HBASE 表中
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(outputPath, hTable);
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
