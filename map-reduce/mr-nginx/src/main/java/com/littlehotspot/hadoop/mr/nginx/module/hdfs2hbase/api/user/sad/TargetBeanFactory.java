package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

/**
 * <h1> 创建target类 </h1>
 * Created by Administrator on 2017-06-26 下午 6:29.
 */
public class TargetBeanFactory {

    public static TextTargetSadActBean getTargetActBean(){
        return new TextTargetSadActBean();

    }

    public static TextTargetSadAttrBean getTargetSadAttrBean(SadType sadType){
        switch (sadType) {
            case PROJECTION:
                return new TextTargetSadAttrBean();
            case DEMAND:
                return new TextTargetSadAttrBean();

        }
        return null;

    }


    public static TextTargetSadRelaBean getTargetSadRelaBean(SadType sadType){
        switch (sadType) {
            case PROJECTION:
                return new TextTargetSadRelaBean();
            case DEMAND:
                return new TextTargetSadRelaBean();
        }
        return null;

    }

}
