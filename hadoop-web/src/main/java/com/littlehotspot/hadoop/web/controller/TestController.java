package com.littlehotspot.hadoop.web.controller;

import com.littlehotspot.hadoop.web.model.Model1;
import com.littlehotspot.hadoop.web.model.Model2;
import com.littlehotspot.hadoop.web.model.SearchModel;
import com.littlehotspot.hadoop.web.service.ITestService;
import com.littlehotspot.hadoop.web.util.StatusCode;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JSONBean;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONString;
import org.mortbay.util.ajax.JSON;
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
        try {
            //设置过滤器
            List<Filter> filters = new ArrayList<>();

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
                    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, fils);
                    filters.add(filterList);
                } else {
                    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, fils);
                    filters.add(filterList);
                }

            }

            String relation = searchModel.getRela();
            FilterList filterList;
            if ("&".equals(relation)) {
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
            } else {
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
            }

            if (filterList != null) {
                List<Map<String, Object>> list = testService.getService(filterList);
                return new StatusCode(10000, "Success", list);
            } else {
                return new StatusCode(10018, "Error");
            }
        }catch (Exception e){
            e.printStackTrace();
            return new StatusCode(10079, "Error");
        }
    }

}
