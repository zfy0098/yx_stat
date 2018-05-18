package com.jiuxiu.yxstat.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.jiuxiu.yxstat.utils.PropertyUtils;

/**
 * Created with IDEA by Zhoufy on 2018/5/11.
 *
 * @author Zhoufy
 */
public class UserDataBase extends BaseDao{


    public UserDataBase(){
        String url = PropertyUtils.getValue("nextjoy.datasource.user.url");
        String username = PropertyUtils.getValue("nextjoy.datasource.user.username");
        String password = PropertyUtils.getValue("nextjoy.datasource.user.password");
        DruidDataSource dataSource = ConnectionFactory.getInstance().getDruidDataSource(username , password , url);
        super.setDruidDataSource(dataSource);
    }
}
