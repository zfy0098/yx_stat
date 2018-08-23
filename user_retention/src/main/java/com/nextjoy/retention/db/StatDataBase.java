package com.nextjoy.retention.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.nextjoy.retention.utils.PropertyUtils;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class StatDataBase extends BaseDao{

    private static DruidDataSource dataSource;

    public StatDataBase(){
        synchronized (this){
            if(dataSource == null){
               String url = PropertyUtils.getValue("nextjoy.datasource.stat.url");
               String username = PropertyUtils.getValue("nextjoy.datasource.stat.username");
               String password = PropertyUtils.getValue("nextjoy.datasource.stat.password");
               dataSource = ConnectionFactory.getInstance().getDruidDataSource(username , password , url);
           }
        }
        super.setDruidDataSource(dataSource);
    }
}
