package com.jiuxiu.yxstat.dao;

import com.jiuxiu.yxstat.db.ConfigDataBase;

/**
 * Created with IDEA by Zhoufy on 2018/5/15.
 *
 * @author Zhoufy
 */
public class ConfigDao  extends ConfigDataBase{


    private static ConfigDao configDao = null;

    private ConfigDao(){}

    public static ConfigDao getInstance(){
        if(configDao == null){
            configDao =  new ConfigDao();
        }
        return configDao;
    }


    public void init(){
        String sql = "select * from app_info limit 1 ";
        log.info(queryForList(sql , null).toString());
    }

}
