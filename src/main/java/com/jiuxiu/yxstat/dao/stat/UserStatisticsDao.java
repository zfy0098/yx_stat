package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

/**
 * Created by ZhouFy on 2018/6/13.
 *
 * @author ZhouFy
 */
public class UserStatisticsDao extends StatDataBase {

    private static UserStatisticsDao userStatisticsDao;

    private UserStatisticsDao(){}

    public static UserStatisticsDao getInstance(){
        if(userStatisticsDao == null){
            userStatisticsDao = new UserStatisticsDao();
        }
        return userStatisticsDao;
    }



    public void saveUserStatistics(Object[] obj){
        String sql = "insert into user_statistics (login_count, register_count, guest_login_count, guest_register_count, date )" +
                " values (?,?,?,?, now()) on duplicate key update login_count = ? , register_count = ? , guest_login_count = ? , guest_register_count = ?";
        executeSql(sql , obj);
    }

}
