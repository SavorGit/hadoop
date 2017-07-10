package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.demand.UserDemand;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.projection.UserProjection;

/**
 * <h1> 创建target类 </h1>
 * Created by Administrator on 2017-06-26 下午 6:29.
 */
public class TargetBeanFactory {

    public static TextTargetSadActBean getTargetActBean(){
        return new TextTargetSadActBean();

    }

    public static UserSad getTargetUserSadBean(SadType sadType){
        switch (sadType) {
            case PROJECTION:
                return new UserProjection();
            case DEMAND:
                return new UserDemand();

        }
        return null;

    }

}
