package com.littlehotspot.hadoop.web.controller;

import com.littlehotspot.hadoop.web.model.HBasePageModel;
import com.littlehotspot.hadoop.web.model.Model1;
import com.littlehotspot.hadoop.web.model.Model2;
import com.littlehotspot.hadoop.web.model.SearchModel;
import com.littlehotspot.hadoop.web.service.ITestService;
import com.littlehotspot.hadoop.web.util.HBaseUtil;
import com.littlehotspot.hadoop.web.util.StatusCode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <h1> TestController </h1>
 * Created by chenxiaoqiong on 2016/12/22 0022 下午 8:42.
 */
@Controller
@RequestMapping(value = "/test")
public class TestController {

    @Autowired
    ITestService testService;

    @RequestMapping(value = "/find", method = RequestMethod.POST, produces = "application/json")
    @ResponseBody
    public StatusCode find(@RequestBody SearchModel searchModel) {
        if (searchModel == null) {
            return new StatusCode(10018, "Parameter Error", null);
        }
        try {

            FilterList filterList = null;

            String relation = searchModel.getRela();

            //设置过滤器
            List<Filter> filters = new ArrayList<>();
            if ("&".equals(relation) || "|".equals(relation)) {

                List<Model1> model1s = searchModel.getCons();

                for (Model1 model1 : model1s) {
                    List<Filter> fils = new ArrayList<>();

                    List<Model2> model2s = model1.getCons();
                    for (Model2 model2 : model2s) {
                        BinaryComparator comp = new BinaryComparator(Bytes.toBytes(model2.getValue()));
                        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                                Bytes.toBytes(model2.getFamily()),
                                Bytes.toBytes(model2.getColumn()),
                                CompareFilter.CompareOp.valueOf(model2.getOperate()),
                                comp);
                        fils.add(filter);
                    }

                    String rela = model1.getRela();
                    if ("&".equals(rela)) {
                        FilterList filList = new FilterList(FilterList.Operator.MUST_PASS_ALL, fils);
                        filters.add(filList);
                    } else {
                        FilterList filList = new FilterList(FilterList.Operator.MUST_PASS_ONE, fils);
                        filters.add(filList);
                    }

                }

                if ("&".equals(relation)) {
                    filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
                } else {
                    filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                }


            }

            int pageSize = 50;
            if (searchModel.getPageSize() > 0) {
                pageSize = searchModel.getPageSize();
            }
            int pageIndex = 1;
            if (searchModel.getPageIndex() > 0) {
                pageIndex = searchModel.getPageIndex();
            }
            String startRow = null;
            if (StringUtils.isNotBlank(searchModel.getStartRow())) {
                startRow = searchModel.getStartRow();
            }

            List<Map<String, Object>> list = testService.getService(filterList, startRow, pageSize, pageIndex, searchModel.getTableName());

            HBasePageModel model = new HBasePageModel(pageSize);
            model.setPageSize(pageSize);
            model.setPageIndex(pageIndex);
            long totalCount = HBaseUtil.rowCount(searchModel.getTableName());
            model.setTotalCount(totalCount);
            long pageCount = totalCount/pageSize;
            if(totalCount%pageSize > 0){
                pageCount += 1;
            }
            model.setPageCount(pageCount);

            if (startRow != null) {
                model.setStartRowKey(startRow);
                model.setPageSize(pageSize - 1);
            }
            if (list != null && list.size() > 0) {
                model.setEndRowKey(list.get(list.size() - 1).get("rowKey").toString());
                model.setResultList(list);
            }

            model.setHasNextPage();

            return new StatusCode(10000, "Success", model);

        } catch (Exception e) {
            e.printStackTrace();
            return new StatusCode(10079, "Service Error");
        }
    }

}
