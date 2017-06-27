package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection.TargetUserProjectionEndBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection.TargetUserProjectionStartBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection.TextTargetUserProjectionAttrBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection.TextTargetUserProjectionRelaBean;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-26 下午 6:29.
 */
public class TargetBeanFactory {

    public static TextTargetSadActBean getTargetActBean(SadActType sadType){
        switch (sadType) {
            case START_PRO:
                return new TargetUserProjectionStartBean();
            case END_PRO:
                return new TargetUserProjectionEndBean();
            case START_DEM:
                break;
            case END_DEM:
                break;
        }
        return null;

    }

    public static TextTargetSadAttrBean getTargetSadAttrBean(SadType sadType){
        switch (sadType) {
            case PROJECTION:
                return new TextTargetUserProjectionAttrBean();
            case DEMAND:

        }
        return null;

    }


    public static TextTargetSadRelaBean getTargetSadRelaBean(SadType sadType){
        switch (sadType) {
            case PROJECTION:
                return new TextTargetUserProjectionRelaBean();
            case DEMAND:
        }
        return null;

    }

}
