package com.littlehotspot.hbase;

import com.littlehotspot.util.Argument;
import com.littlehotspot.util.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *@Author 刘飞飞
 *@Date 2017/7/19 13:32
 */
public class UserMr extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
        String hdfsOutputPath = Constant.CommonVariables.getParameterValue(Argument.OutputPath);
        String excelTargetPath=Constant.CommonVariables.getParameterValue(Argument.ExcelOutPath);

        Configuration conf = HBaseConfiguration.create(this.getConf());
        Job job = Job.getInstance(conf,"read_data_from_hbase");
        job.setJarByClass(UserMr.class);
        job.setSpeculativeExecution(false);

        Scan scan = new Scan();
        scan.addColumn(UserMapper.USER_ATTR_FAMILY.getBytes(),"device_id".getBytes());
        scan.addColumn(UserMapper.USER_ATTR_FAMILY.getBytes(),"m_type".getBytes());
        scan.addColumn(UserMapper.USER_ATTR_FAMILY.getBytes(),"m_ machine".getBytes());

        scan.addColumn(UserMapper.USER_ACTION_FAMILY.getBytes(),"read_count".getBytes());
        scan.addColumn(UserMapper.USER_ACTION_FAMILY.getBytes(),"f_down_time".getBytes());
        scan.addColumn(UserMapper.USER_ACTION_FAMILY.getBytes(),"f_proje_time".getBytes());
        scan.addColumn(UserMapper.USER_ACTION_FAMILY.getBytes(),"f_dema_time".getBytes());
        TableMapReduceUtil.initTableMapperJob("user_basic",scan,UserMapper.class,SortKeyPair.class,Text.class, job,true);

        Path outputPath = new Path(hdfsOutputPath);
        FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setReducerClass(UserReduce.class);

        job.setSortComparatorClass(SortComparatorClass.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        boolean status = job.waitForCompletion(true);
        if (!status) {
            throw new Exception("MapReduce task execute failed.........");
        }
//        System.out.println(UserReduce.userVoList.size());
//        System.out.println(UserReduce.userVoList);
//        ExportUserInfo.exportUserInfo(excelTargetPath,userVoList);
        return 1;
    }

    private static class UserMapper extends TableMapper<SortKeyPair,Text> {
        private static String USER_ATTR_FAMILY = "attr";
        private static String USER_ACTION_FAMILY = "acti";
        private static String SPLIT = "|";

        @Override
        protected void map(ImmutableBytesWritable key, Result res, Context context) throws IOException, InterruptedException {
            if (res == null || res.isEmpty()) return;
            //过滤不必要数据
            byte[] attrFamily = USER_ATTR_FAMILY.getBytes();
            byte[] actiFamily = USER_ACTION_FAMILY.getBytes();
            String deviceId = Bytes.toString(res.getValue(attrFamily, "device_id".getBytes()));
            Configuration config = context.getConfiguration();
            if (StringUtils.isNotBlank(config.get(deviceId))) {
                return;
            }
            //attr
            String mType = Bytes.toString(res.getValue(attrFamily, "m_type".getBytes()));
            String mMachine = Bytes.toString(res.getValue(attrFamily, "m_ machine".getBytes()));

            //action
            String readCount = StringUtils.defaultIfBlank(Bytes.toString(res.getValue(actiFamily, "read_count".getBytes())), "0");
            String fDownTime = Bytes.toString(res.getValue(actiFamily, "f_down_time".getBytes()));
            String fProjeTime = Bytes.toString(res.getValue(actiFamily, "f_proje_time".getBytes()));
            String fDemaTime = Bytes.toString(res.getValue(actiFamily, "f_dema_time".getBytes()));


            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(deviceId).append(SPLIT);
            stringBuilder.append(mType).append(SPLIT);
            stringBuilder.append(mMachine).append(SPLIT);
            stringBuilder.append(readCount).append(SPLIT);
            stringBuilder.append(fDownTime).append(SPLIT);
            stringBuilder.append(fProjeTime).append(SPLIT);
            stringBuilder.append(fDemaTime);
            try {
                context.write(new SortKeyPair(Integer.parseInt(readCount)), new Text(stringBuilder.toString()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

        private static class UserReduce extends Reducer<SortKeyPair, Text, Text, Text> {
            private int num=0;
            private static List<String> userVoList=new ArrayList<>();
            @Override
            protected void reduce(SortKeyPair key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
                try {
                    Iterator<Text> iterator = value.iterator();
                    while (iterator.hasNext()){
                        Text item = iterator.next();
                        if (item == null) {
                            continue;
                        }
                        if(num<100){
                            userVoList.add(num,item.toString());
                        }
                        num++;
                        context.write(null,item);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            protected void  cleanup(Context context){
                System.out.println(userVoList.size());
                System.out.println(userVoList);
                System.out.println(num);
            }
        }

    }

