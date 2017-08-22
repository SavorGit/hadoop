package com.littlehotspot.hadoop.web.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-18 下午 5:09.
 */
@Getter
@Setter
public class HBasePageModel implements Serializable {
    private static final long serialVersionUID = 330410716100946538L;

    private long pageSize = 10;
    private long pageIndex = 0;
    private String startRowKey = null;

    private long pageCount = 0;
    private long totalCount = 0;
    private boolean hasNextPage = true;
    private String endRowKey = null;
    private List<Map<String, Object>> resultList = new ArrayList<>();

    public HBasePageModel(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * 获取是否有下一页
     *
     * @return
     */
    public void setHasNextPage() {
        if ((this.pageIndex - 1) * this.pageSize + this.resultList.size() < this.totalCount) {
            this.hasNextPage = true;
        } else {
            this.hasNextPage = false;
        }
    }
}
