package com.jiuxiu.yxstat.dao;

import com.jiuxiu.yxstat.db.UserDataBase;

/**
 * Created with IDEA by Zhoufy on 2018/5/11.
 *
 * @author Zhoufy
 */
public class UserDao extends UserDataBase {


    private static UserDao userDao = null;
    private UserDao(){}

    public static UserDao getUserDao(){
        if(userDao == null){
            return new UserDao();
        }
        return userDao;
    }


    public void init(){
        String sql = "select * from user_account";
        log.info(queryForList(sql , null).toString());
    }
}
