package com.jiuxiu.yxstat.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.jiuxiu.yxstat.utils.PropertyUtils;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class StatDataBase extends BaseDao{


    public StatDataBase(){
        String url = PropertyUtils.getValue("nextjoy.datasource.stat.url");
        String username = PropertyUtils.getValue("nextjoy.datasource.stat.username");
        String password = PropertyUtils.getValue("nextjoy.datasource.stat.password");
        DruidDataSource dataSource = ConnectionFactory.getInstance().getDruidDataSource(username , password , url);
        super.setDruidDataSource(dataSource);
    }
}
