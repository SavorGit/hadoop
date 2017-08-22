package com.littlehotspot.hadoop.web.model;


import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-18 上午 11:07.
 */
@Getter
@Setter
public class SearchModel {
    private String tableName;
    private String startRow; //从哪一行开始查
    private int pageSize; //每一个多少行
    private int pageIndex; //第几页

    private String rela;
    private List<Model1> cons;

}

