package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/18.
 *
 * @author Zhoufy
 */
public class AppInstallDao extends StatDataBase implements Serializable{


    private static AppInstallDao appInstallDao = null;


    private AppInstallDao(){}

    public static AppInstallDao getAppInstallDao(){
        if(appInstallDao == null){
            return new AppInstallDao();
        }
        return appInstallDao;
    }



    public Map<String,Object> getDeviceActiveInfoByimei(String imei){
        String sql = "select id , app_id , app_child_id , channel_id , app_channel_id , imei , idfa , " +
                " os , ip , device_name , mac , install_time from app_install where imei = ?";
        return queryForMap(sql , new Object[]{imei});
    }


    public Map<String,Object> getDeviceActiveInfoByIdfa(String idfa){
        String sql = "select id , app_id , app_child_id , channel_id , app_channel_id , imei , idfa , " +
                " os , ip , device_name , mac , install_time from app_install where idfa = ?";
        return queryForMap(sql , new Object[]{idfa});
    }


    public int saveDeviceActiveInfo(Object[] obj){
        String sql = "insert into app_install ( app_id , app_child_id , channel_id , app_channel_id , package_id , imei , idfa , " +
                "os  , device_os_ver , ip , device_name , mac , install_time) values (?,?,?,?,?,?,?,?,?,?,?,? , ?)";
        return executeSql(sql , obj);
    }



    public List<Map<String,Object>> deviceActiveList(Object[] obj){
        String sql = "select id , app_id , app_child_id , channel_id , app_channel_id , imei , idfa , " +
                " os , ip , device_name , mac , install_time from app_install where ip=? and device_name=? and device_os_ver=?";
        return queryForList(sql , obj);
    }


    /**
     *
     *
     *
     *    public void saveDeviceInfo(JSONObject json) {

             int appid = json.getInt("appid");
             int app_child_id = json.getInt("child_id");
             int channel_id = json.getInt("channel_id");
             int app_channel_id = json.getInt("app_channel_id");
             int package_id = json.getInt("package_id");
             String imei = json.getString("imei");
             String idfa = json.getString("idfa");
             int os = json.getInt("os");
             String device_os_ver = json.getString("device_os_ver");
             String ip = json.getString("client_ip");
             String device_name = json.getString("device_name");
             String mac = json.getString("mac");
             long install_time = System.currentTimeMillis() / 1000;

             Object[] obj = new Object[]{appid, app_child_id, channel_id, app_channel_id, package_id, imei, idfa,
             os, device_os_ver, ip, device_name, mac, install_time};
             appInstallDao.saveDeviceActiveInfo(obj);
     }
     *
     */


}
