package com.littlehotspot.export;

import com.littlehotspot.model.UserReadOrderAttrBean;
import com.littlehotspot.util.Argument;
import com.littlehotspot.util.Constant;
import com.littlehotspot.util.excel.ExcelBean;
import com.littlehotspot.util.excel.ExcelUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *@Author 刘飞飞
 *@Date 2017/7/21 11:35
 */
public class ExportTool extends Configured implements Tool {
    private static String USER_ATTR_FAMILY = "attr";
    private static int MAX_RESULT_SIZE=100;
    @Override
    public int run(String[] args) throws Exception {
        Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
        String excelTargetPath=Constant.CommonVariables.getParameterValue(Argument.ExcelOutPath);
        List<UserReadOrderAttrBean> userOrderList=getUserOrderList(this.getConf());
        exportUserInfo(excelTargetPath,userOrderList);
        return 0;
    }


    /**
     * 导出excel
     */
    private void exportUserInfo(String targetFilePath,List<UserReadOrderAttrBean> userOrders){
        String tempFilePath="temp/excel-temp.xlsx";
        ExcelBean excelBean=new ExcelBean();
        excelBean.setStartRowNum(1);
        excelBean.setBorder(true);
        List<List> rows=new ArrayList<>();
        List cell;
        for(int i=0;i<userOrders.size();i++){
            cell=new ArrayList();
            cell.add(userOrders.get(i).getUserId());
            cell.add(userOrders.get(i).getMType());
            cell.add(userOrders.get(i).getMMachine());
            cell.add(userOrders.get(i).getReadCount());
            String fDownTime=userOrders.get(i).getFDownTime();
            if(StringUtils.isNotBlank(fDownTime)){
                fDownTime= DateFormatUtils.format(new Date(Long.parseLong(fDownTime)), Constant.DATA_FORMAT_2);
            }
            String fProjeTime=userOrders.get(i).getFProjeTime();
            if(StringUtils.isNotBlank(fProjeTime)){
                fProjeTime= DateFormatUtils.format(new Date(Long.parseLong(fProjeTime)), Constant.DATA_FORMAT_2);
            }
            String fDemaTime=userOrders.get(i).getFDemaTime();
            if(StringUtils.isNotBlank(fDemaTime)){
                fDemaTime= DateFormatUtils.format(new Date(Long.parseLong(fDemaTime)), Constant.DATA_FORMAT_2);
            }
            cell.add(fDownTime);
            cell.add(fProjeTime);
            cell.add(fDemaTime);
            rows.add(cell);
        }
        excelBean.setData(rows);
        ExcelUtil.writeToExcelTemplet(targetFilePath,tempFilePath,excelBean);
    }


    private List<UserReadOrderAttrBean> getUserOrderList(Configuration confs) throws IOException {
        List<UserReadOrderAttrBean> userOrderList=new ArrayList<>();
        Configuration conf = HBaseConfiguration.create(confs);
        HTable table = new HTable(conf, "user_read_order");
        Scan scan = new Scan();
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"user_id".getBytes());
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"m_type".getBytes());
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"m_ machine".getBytes());
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"read_count".getBytes());
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"f_down_time".getBytes());
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"f_proje_time".getBytes());
        scan.addColumn(USER_ATTR_FAMILY.getBytes(),"f_dema_time".getBytes());
        scan.setFilter(new PageFilter(MAX_RESULT_SIZE));
        ResultScanner scanner = table.getScanner(scan);
        UserReadOrderAttrBean bean;
        for (Result res : scanner) {
            bean=convert(res);
            userOrderList.add(bean);
        }
        scanner.close();
        return userOrderList;
    }


    private UserReadOrderAttrBean convert(Result res){
        String userId = Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("user_id")));
        String mType = Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("m_type")));
        String mMachine = Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("m_ machine")));
        String readCount = StringUtils.defaultIfBlank(Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("read_count"))), "0");
        String fDownTime = Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("f_down_time")));
        String fProjeTime = Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("f_proje_time")));
        String fDemaTime = Bytes.toString(res.getValue(Bytes.toBytes(USER_ATTR_FAMILY), Bytes.toBytes("f_dema_time")));

        UserReadOrderAttrBean userOrder=new UserReadOrderAttrBean();
        userOrder.setUserId(userId);
        userOrder.setMType(mType);
        userOrder.setMMachine(mMachine);
        userOrder.setReadCount(readCount);
        userOrder.setFDownTime(fDownTime);
        userOrder.setFProjeTime(fProjeTime);
        userOrder.setFDemaTime(fDemaTime);
        return userOrder;
    }


}
