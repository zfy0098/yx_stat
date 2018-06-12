package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.StatActivationStatisticsDao;
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
public class StartupCountDataService implements Serializable{

    private static StatActivationStatisticsDao statActivationStatisticsDao = StatActivationStatisticsDao.getInstance();

    /**
     * 计算启动数
     *
     * @param javaRDD 数据
     */
    public static void startupCount(JavaRDD<JSONObject> javaRDD) {

        javaRDD.mapToPair(new PairFunction<JSONObject, String, Integer>() {
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
                int keyLength = 5;
                if (keys.length == keyLength) {
                    String packageID = keys[0];
                    String childID = keys[1];
                    String appChannelID = keys[2];
                    String channelID = keys[3];
                    String appid = keys[4];
                    statActivationStatisticsDao.savePackageIdStartUpCount(new Object[]{packageID, tuple2._2, tuple2._2});
                    statActivationStatisticsDao.saveChildIDStartUpCount(new Object[]{childID, tuple2._2, tuple2._2});
                    statActivationStatisticsDao.saveAppChannelIdStartUpCount(new Object[]{appChannelID, tuple2._2, tuple2._2});
                    statActivationStatisticsDao.saveChannelIdStartUpCount(new Object[]{channelID, tuple2._2, tuple2._2});
                    statActivationStatisticsDao.saveAppIdStartUpCount(new Object[]{appid, tuple2._2, tuple2._2});
                }
            }
        });
    }
}
