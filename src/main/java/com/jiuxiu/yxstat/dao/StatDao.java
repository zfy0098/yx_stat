package com.jiuxiu.yxstat.dao;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class StatDao extends StatDataBase implements Serializable{

    private static StatDao statDao = null;

    private StatDao (){}

    public static StatDao getStatDao(){
        if(statDao == null){
            return new StatDao();
        }
        return statDao;
    }


    public Map<String,Object>  getAppClickCountByAppIDAndOS(int appid , int os){
        String sql = "select * from stat_app_click where appid = ? and os = ?";
        return queryForMap(sql , new Object[]{appid , os});
    }

    public List<Map<String,Object>> getAppClickCountByOS(int os){
        String sql = "select * from stat_app_click where os =?";
        return  queryForList(sql , new Object[]{os});
    }


    public int saveDeviceInstall(Object[] list){
        String sql = "insert ignore into stat_device_install (imei , os , activedate) values (?,?,now())";
        return executeSql(sql , list);
    }


    public int updateAppClickCount(Object[] objects){
        String sql = "insert into stat_app_click (appid , click_count , os) values (?,?,?) " +
                "on duplicate key update click_count=?";
        return executeSql( sql , objects);
    }






}
