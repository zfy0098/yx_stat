package com.jiuxiu.yxstat.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.jiuxiu.yxstat.utils.PropertyUtils;


/**
 * Created with IDEA by Zhoufy on 2018/5/11.
 *
 * @author Zhoufy
 */
public class ConfigDataBase extends BaseDao{

    public ConfigDataBase(){
        String url = PropertyUtils.getValue("nextjoy.datasource.config.url");
        String username = PropertyUtils.getValue("nextjoy.datasource.config.username");
        String password = PropertyUtils.getValue("nextjoy.datasource.config.password");
        DruidDataSource dataSource = ConnectionFactory.getInstance().getDruidDataSource(username , password , url);
        super.setDruidDataSource(dataSource);
    }
}
