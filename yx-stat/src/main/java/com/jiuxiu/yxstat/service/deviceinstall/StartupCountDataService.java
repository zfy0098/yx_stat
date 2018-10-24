package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChildDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatPackageIdDeviceActiveDao;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Serializable;
import scala.Tuple2;

/**
 * Created by ZhouFy on 2018/6/12.
 *
 * @author ZhouFy
 */
public class StartupCountDataService implements Runnable ,  Serializable {

    /**
     * 天统计DAO类对象
     */
    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();


    private JavaRDD<JSONObject> javaRDD;

    StartupCountDataService(JavaRDD<JSONObject> javaRDD){
        this.javaRDD = javaRDD;
    }

    /**
     * 计算启动数
     */
    @Override
    public void run() {


        javaRDD.filter(json ->{
            int x = json.getInt("os");
            return x == ServiceConstant.ANDROID_OS;
        }).mapToPair(new PairFunction<JSONObject, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(JSONObject json) {
                StringBuffer key = new StringBuffer();
                key.append(json.getInt("package_id"));
                key.append("#");
                key.append(json.getInt("child_id"));
                key.append("#");
                key.append(json.getInt("app_channel_id"));
                key.append("#");
                key.append(json.getInt("channel_id"));
                key.append("#");
                key.append(json.getInt("appid"));
                long second = json.getLong("ts");
                String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
                String toDay = DateUtil.secondToDateString(second, DateUtil.YYYY_MM_DD);
                key.append("#");
                key.append(time);
                key.append("#");
                key.append(toDay);
                return new Tuple2<>(key.toString(), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) {
                String[] keys = tuple2._1.split("#");
                int keyLength = 7;
                if (keys.length == keyLength) {
                    String packageID = keys[0];
                    String childID = keys[1];
                    String appChannelID = keys[2];
                    String channelID = keys[3];
                    String appid = keys[4];

                    String time = keys[5];
                    String today = keys[6];

                    /*
                     *   保存天统计数据
                     */
                    statChildDeviceActiveDao.saveChildIDStartUpCount(today, appid, childID, tuple2._2);
                    statAppChannelIdDeviceActiveDao.saveAppChannelIdStartUpCount(today, appid, childID, channelID, appChannelID, tuple2._2);
                    statChannelIdDeviceActiveDao.saveChannelIdStartUpCount(today, appid, childID, channelID, tuple2._2);
                    statAppIdDeviceActiveDao.saveAppIdStartUpCount(today, appid, tuple2._2);
                    statPackageIdDeviceActiveDao.savePackageIdStartUpCount(today, appid, childID, channelID, appChannelID, packageID, tuple2._2);

                    /*
                     *  保存分钟统计数据
                     */
                    statChildDeviceActiveDao.saveChildIDMinuteStartUpCount(time, appid, childID, tuple2._2);
                    statAppChannelIdDeviceActiveDao.saveAppChannelIDMinuteStartUpCount(time, appid, childID, channelID, appChannelID, tuple2._2);
                    statChannelIdDeviceActiveDao.saveChannelIDMinuteStartUpCount(time, appid, childID, channelID, tuple2._2);
                    statAppIdDeviceActiveDao.saveAppIDMinuteStartUpCount(time, appid, tuple2._2);
                    statPackageIdDeviceActiveDao.savePackageIDMinuteStartUpCount(time, appid, childID, channelID, appChannelID, packageID, tuple2._2);
                }
            }
        });
    }
}
