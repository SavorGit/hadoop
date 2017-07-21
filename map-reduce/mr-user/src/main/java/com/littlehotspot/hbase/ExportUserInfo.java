package com.littlehotspot.hbase;

import com.littlehotspot.util.Constant;
import com.littlehotspot.util.excel.ExcelBean;
import com.littlehotspot.util.excel.ExcelUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *@Author 刘飞飞
 *@Date 2017/7/18 16:18
 */
public class ExportUserInfo {
    /**
     * 导出excel
     */
    public static void exportUserInfo(String targetFilePath,List<String> userStrs){
        String tempFilePath="/temp/excel-temp.xlsx";
        ExcelBean excelBean=new ExcelBean();
        excelBean.setStartRowNum(1);
        excelBean.setBorder(true);
        List<List> rows=new ArrayList<>();
        List cell;
        for(int i=0;i<userStrs.size();i++){
            cell=new ArrayList();
            String[] ss=userStrs.get(i).split("\\|",-1);
            cell.add(ss[0]);
            cell.add(ss[1]);
            cell.add(ss[2]);
            cell.add(ss[3]);
            String fDownTime=ss[4];
            if(StringUtils.isNotBlank(fDownTime)){
                fDownTime= DateFormatUtils.format(new Date(Long.parseLong(fDownTime)), Constant.DATA_FORMAT_2);
            }
            String fProjeTime=ss[5];
            if(StringUtils.isNotBlank(fProjeTime)){
                fProjeTime= DateFormatUtils.format(new Date(Long.parseLong(fProjeTime)), Constant.DATA_FORMAT_2);
            }
            String fDemaTime=ss[6];
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

}
