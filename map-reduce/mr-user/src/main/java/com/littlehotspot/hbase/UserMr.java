package com.littlehotspot.hbase;

import com.littlehotspot.model.UserReadOrderAttrBean;
import com.littlehotspot.model.UserReadOrderBean;
import com.littlehotspot.util.Argument;
import com.littlehotspot.util.Constant;
import com.littlehotspot.util.hbase.HBaseHelper;
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
import java.util.Iterator;

/**
 *@Author 刘飞飞
 *@Date 2017/7/19 13:32
 */
public class UserMr extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
        String hdfsOutputPath = Constant.CommonVariables.getParameterValue(Argument.OutputPath);
        String isFilter= Constant.CommonVariables.getParameterValue(Argument.IsFilter);
        Configuration conf = HBaseConfiguration.create(this.getConf());
        conf.set("isFilter",isFilter);
        filterUserId(conf);
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
        TableMapReduceUtil.initTableMapperJob("user_basic",scan,UserMapper.class,Text.class,Text.class, job,true);

        Path outputPath = new Path(hdfsOutputPath);
        FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setReducerClass(UserReduce.class);

//        job.setSortComparatorClass(SortComparatorClass.class);
//        job.setPartitionerClass(NaturalKeyPartitioner.class);
//        job.setGroupingComparatorClass(NaturalKeyGroupComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        boolean status = job.waitForCompletion(true);
        if (!status) {
            throw new Exception("MapReduce task execute failed.........");
        }
        return 1;
    }

    private static class UserMapper extends TableMapper<Text,Text> {
        private static String USER_ATTR_FAMILY = "attr";
        private static String USER_ACTION_FAMILY = "acti";
        private static String SPLIT = "|";

        @Override
        protected void map(ImmutableBytesWritable key, Result res, Context context) throws IOException, InterruptedException {
            if (res == null || res.isEmpty()) return;
            //过滤不必要数据
            byte[] attrFamily = Bytes.toBytes(USER_ATTR_FAMILY);
            byte[] actiFamily = Bytes.toBytes(USER_ACTION_FAMILY);
            String deviceId = Bytes.toString(res.getValue(attrFamily, Bytes.toBytes("device_id")));
            Configuration config = context.getConfiguration();
            if (StringUtils.isNotBlank(config.get(deviceId))) {
                System.out.println(config.get(deviceId));
                return;
            }
            //attr
            String mType = Bytes.toString(res.getValue(attrFamily, Bytes.toBytes("m_type")));
            String mMachine = Bytes.toString(res.getValue(attrFamily, Bytes.toBytes("m_ machine")));

            //action
            String readCount = StringUtils.defaultIfBlank(Bytes.toString(res.getValue(actiFamily, Bytes.toBytes("read_count"))), "0");
            String fDownTime = Bytes.toString(res.getValue(actiFamily, Bytes.toBytes("f_down_time")));
            String fProjeTime = Bytes.toString(res.getValue(actiFamily, Bytes.toBytes("f_proje_time")));
            String fDemaTime = Bytes.toString(res.getValue(actiFamily, Bytes.toBytes("f_dema_time")));

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(deviceId).append(SPLIT);
            stringBuilder.append(mType).append(SPLIT);
            stringBuilder.append(mMachine).append(SPLIT);
            stringBuilder.append(readCount).append(SPLIT);
            stringBuilder.append(fDownTime).append(SPLIT);
            stringBuilder.append(fProjeTime).append(SPLIT);
            stringBuilder.append(fDemaTime);
            String rowkey=(9999999999L-Long.parseLong(readCount))+SPLIT+deviceId;
            try {
                context.write(new Text(rowkey), new Text(stringBuilder.toString()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

        private static class UserReduce extends Reducer<Text, Text, Text, Text> {
            private HBaseHelper hbaseHelper;
            @Override
            protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
                try {
                    Iterator<Text> iterator = value.iterator();
                    UserReadOrderAttrBean orderBean=null;
                    UserReadOrderBean userReadOrderBean=new UserReadOrderBean();
                    while (iterator.hasNext()){
                        Text item = iterator.next();
                        if (item == null) {
                            continue;
                        }
                        //输入排序Hbase
                        orderBean=new UserReadOrderAttrBean(item.toString());
                        userReadOrderBean.setUserReadOrderAttrBean(orderBean);
                        userReadOrderBean.setRowKey(key.toString());
                        this.hbaseHelper.insert(userReadOrderBean);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                this.hbaseHelper=new HBaseHelper(conf);
            }
        }

        private void filterUserId(Configuration config){
             if(!StringUtils.equals(config.get("isFilter"),"true")){
                 return;
             }
             String[] userIds=new String[]{
                     "0911C065-D984-46EC-BEFE-53DC424D686D",
                     "1458F88D-4D62-4E25-BA67-05BD55D03BC0",
                     "860076031731317","7151C17E-AA27-4CAB-A582-0D0316B97C9E",
                     "863125031327738","863274036064324","563DB0E2-28D0-4C2B-B38A-21B5B8B5DDB1",
                     "6EEB4D42-19CD-486B-841E-EA4D9C88D7C8","D51057FB-1BC9-4FC7-A2A0-675CAD50B2A1",
                     "866693029843955","353952075610662","5074C764-9105-4849-A489-EDE9F99A30F1",
                     "141D38D-6B2C-43CD-A1B7-A80BD14747EF","8F59450C-220F-4B92-B028-A0484C86F22A",
                     "BA4979F1-34A8-45DE-AC2A-A94076AF1D70","352709083931097","225E39E8-21B0-4F59-9FE4-F93BC549FD3F",
                     "2B930E0E-090D-4E9D-82CE-8E146C7ED6FE","BA589B6A-9D46-498E-8D9E-A5957DD8F935",
                     "1B59F44-403E-488C-B447-64DFE385C3AC","865902032105224","C91FA192-31DD-462E-AE82-3EAB16686080",
                     "861545038107738","1F947DD2-1D96-4D58-B3DD-650DAF098017","8613650310081917",
                     "864446027301522","1BD79BFD-FECB-4BAE-A63A-56E88CDD2EA2","353819080640937",
                     "A093A141-02AE-41F6-811B-33F4B57ED072","351956082010370","355834080071809",
                     "99000712872422","352564072148983","354983075136563","5096028A-AED5-478D-9D2D-10D7FE3A4F57",
                     "438C2BB4-1BF9-47BB-8AAB-28AACFC258B2","861365031081917","5DF33AEE-6F8E-4EDE-96FA-FDD6A1B9D0C1",
                     "5025B967-DE89-47E7-AAC0-5772D22E43CB","C685CEAE-0A63-4A0F-89E1-07533D55B9C9","861353037565679",
                     "5DF33AEE-6F8E-4EDE-96FA-FDD6A1B9D0C1","34BD0B91-8893-459E-95D7-12116821158C","0EC3D99A-0851-4ED2-A664-BF0FDDA11046",
                     "862031030667283","356832070065210","E5D708A4-C38E-48AA-8579-F6BD51BCCC22",
                     "869372029927129","2D00A669-4E2D-4B40-8AEF-1B4F034BC366","867831025266610",
                     "355905072113252","357536060681709","352628062750593",
                     "86106903086897","438C2BB4-1BF9-47BB-8AAB-28AACFC258B2","5B12C40C-EF11-4076-ABA0-1A91B62A13F2",
                     "75213B3C-7016-4DE0-B9AA-4F5F458336C1","738D9081-A037-47D7-BDCB-18DB3774F25E","DD163B65-F341-48AF-89F4-37FD2D9C05DB",
                     "863410031927012"
             };
             for(int i=0;i< userIds.length;i++){
                 config.set(userIds[i],userIds[i]);
             }
        }
    }

