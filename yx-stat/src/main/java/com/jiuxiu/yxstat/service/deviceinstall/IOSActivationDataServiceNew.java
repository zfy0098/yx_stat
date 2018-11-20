package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChildDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatPackageIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.ElasticSearchConfig;
import com.jiuxiu.yxstat.es.deviceinstall.DeviceActivationStatisticsESStorage;
import com.jiuxiu.yxstat.redis.CacheKey;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisDeviceInstallKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by hadoop on 2018/10/12.
 *
 * @author Zhoufy
 */
public class IOSActivationDataServiceNew implements Runnable, Serializable {


    private Logger log = LoggerFactory.getLogger(this.getClass());

    private static String IOSClickIndex = PropertyUtils.getValue("es.ios.click.index");

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();

    /**
     * 天统计DAO类对象
     */
    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();

    private JavaRDD<JSONObject> ios;

    public IOSActivationDataServiceNew(JavaRDD<JSONObject> ios) {
        this.ios = ios;
    }


    @Override
    public void run() {

        JavaRDD<JSONObject> ioss = ios.filter(json -> {

            int appID = json.getInt("appid");
            int childID = json.getInt("child_id");
            int appChannelID = json.getInt("app_channel_id");
            int channelID = json.getInt("channel_id");
            int packageID = json.getInt("package_id");
            String imei = json.getString("imei");

            String idfa = json.getString("idfa");

            long second = json.getLong("ts");
            String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
            String toDay = DateUtil.secondToDateString(second, DateUtil.YYYY_MM_DD);

            boolean flag = false;

            // 查询改imei 是否激活 没有结果 证明该imei 没有激活
            SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei, appID, childID);
            if (searchResponse.getHits().getHits().length == 0) {
                System.out.println("没有激活" + imei + "," + appID + "." + childID + " , " + packageID);
                flag = true;
            }else{

                Map<String,Object> map = searchResponse.getHits().getHits()[0].getSource();

                System.out.println("已经激活" + searchResponse.getHits().getHits().length + ", 数据：" + map.toString());

                appID = Integer.parseInt(map.get("appid").toString());
                childID = Integer.parseInt(map.get("child_id").toString());
                channelID = Integer.parseInt(map.get("channel_id").toString());
                appChannelID = Integer.parseInt(map.get("app_channel_id").toString());
                packageID = Integer.parseInt(map.get("package_id").toString());

                json.put("appid", appID);
                json.put("child_id", childID);
                json.put("app_channel_id", appChannelID);
                json.put("channel_id", channelID);
                json.put("package_id", packageID);
            }

            if (flag) {

                // 获取同ip激活数
                String ip = json.getString("client_ip");
                int ipActiveCount = deviceActivationStatisticsESStorage.getIPActiveCount(ip, appID);

                if (ipActiveCount < ServiceConstant.ACTIVE_COUNT) {

                    String date = null;
                    try {
                        date = DateUtil.getDateAgo(DateUtil.getNowDate("yyyyMMdd"), 1, "yyyyMMdd");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    boolean adClickFlag = false;
                    String deviceOSVer = json.getString("device_os_ver");

                    deviceOSVer = deviceOSVer.replaceAll("[A-Za-z]" , "").trim();

                    String deviceName = json.getString("device_name");

                    System.out.println("date:" + date + ", idfa : " + idfa + " , appid:" + appID + ", ip:" + ip + ", deviceosver:" + deviceOSVer) ;

                    String type = date;
                    SearchResponse adClickInfo = null;
                    if (!StringUtils.isEmpty(idfa)) {
                        adClickInfo = getAdClickInfo(date, appID, idfa);
                        if (adClickInfo.getHits().getHits().length == 0) {
                            String nowDate = DateUtil.getNowDate("yyyyMMdd");
                            type = nowDate;
                            System.out.println("date:" + nowDate + ", idfa : " + idfa + " , appid:" + appID);
                            adClickInfo = getAdClickInfo(nowDate, appID, idfa);
                            if (adClickInfo.getHits().getHits().length > 0) {
                                adClickFlag = true;
                            }
                        } else {
                            adClickFlag = true;
                        }
                    }

                    if(adClickInfo != null && adClickInfo.getHits().getHits().length == 0){
                        type = date;
                        adClickInfo = getAdClickInfo(date, appID, ip, deviceName, deviceOSVer);
                        if (adClickInfo.getHits().getHits().length == 0) {
                            String nowDate = DateUtil.getNowDate("yyyyMMdd");
                            type = nowDate;
                            System.out.println("date:" + nowDate + ", appid:" + appID + ", ip:" + ip + ", deviceosver:" + deviceOSVer);
                            adClickInfo = getAdClickInfo(date, appID, ip, deviceName, deviceOSVer);
                            if (adClickInfo.getHits().getHits().length > 0) {
                                adClickFlag = true;
                            }
                        } else {
                            adClickFlag = true;
                        }
                    }

                    if (adClickFlag) {

                        SearchHits searchHits = adClickInfo.getHits();
                        SearchHit[] searchHit = searchHits.getHits();
                        SearchHit hit = searchHit[0];

                        System.out.println("获取ios click info 成功" + hit.getSource().toString()) ;

                        deviceActivationStatisticsESStorage.updateAndroidClickInfo(type, hit.getId());

                        Map<String,Object> map = hit.getSource();

                        appID = Integer.parseInt(map.get("appid").toString());
                        childID = Integer.parseInt(map.get("child_id").toString());
                        channelID = Integer.parseInt(map.get("channel_id").toString());
                        appChannelID = Integer.parseInt(map.get("app_channel_id").toString());
                        packageID = Integer.parseInt(map.get("package_id").toString());

                        json.put("appid", appID);
                        json.put("child_id", childID);
                        json.put("app_channel_id", appChannelID);
                        json.put("channel_id", channelID);
                        json.put("package_id", packageID);

                        JSONObject clickInfo = new JSONObject();
                        clickInfo.putAll(json);
                        clickInfo.put("ad_click" , JSONObject.fromObject(map));
                        JedisUtils.listAdd(JedisPoolConfigInfo.adClickPoolKey, "click_device", clickInfo.toString());
                    }
                }

                // 没有激活
                deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json, appID);

                //将新的id 写入到redis 中
                JedisUtils.set(JedisPoolConfigInfo.adClickPoolKey,  CacheKey.IOS_CLICK_INFO + imei + ":" + appID, json.toString(), ServiceConstant.ACTIVATION_INFO_EXPIRE_TIME);


                //  保存 app id 激活数  (当天)
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

                //  保存 app id 激活数 (当前时间 ，每10分钟一次数据)
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

            }

            // 启动设备数
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);


            // 启动设备数
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

            return true;
        });

        /*
         *   计算启动数
         */
        ioss.mapToPair(json -> {
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
        }).reduceByKey((integer , integer2) ->
                integer + integer2
        ).foreach(tuple2 -> {
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

                System.out.println("计算启动次数 appid: " + appid + " , cid " + childID + " , channel " + channelID + ", acid :" + appChannelID + " ,  packageid " + packageID);

                SaveDataUtils.saveDeviceInstallData(today, Integer.parseInt(appid), Integer.parseInt(childID), Integer.parseInt(channelID)
                        , Integer.parseInt(appChannelID), Integer.parseInt(packageID));
                SaveMinuteDataUtils.saveMinuteDeviceInstallData(time, Integer.parseInt(appid), Integer.parseInt(childID)
                        , Integer.parseInt(channelID), Integer.parseInt(appChannelID), Integer.parseInt(packageID));

                /*
                 *   保存天统计数据
                 */
                statAppIdDeviceActiveDao.saveAppIdStartUpCount(today, appid, tuple2._2);
                statChildDeviceActiveDao.saveChildIDStartUpCount(today, appid, childID, tuple2._2);
                statChannelIdDeviceActiveDao.saveChannelIdStartUpCount(today, appid, childID, channelID, tuple2._2);
                statAppChannelIdDeviceActiveDao.saveAppChannelIdStartUpCount(today, appid, childID, channelID, appChannelID, tuple2._2);
                statPackageIdDeviceActiveDao.savePackageIdStartUpCount(today, appid, childID, channelID, appChannelID, packageID, tuple2._2);

                /*
                 *  保存分钟统计数据
                 */
                statAppIdDeviceActiveDao.saveAppIDMinuteStartUpCount(time, appid, tuple2._2);
                statChildDeviceActiveDao.saveChildIDMinuteStartUpCount(time, appid, childID, tuple2._2);
                statChannelIdDeviceActiveDao.saveChannelIDMinuteStartUpCount(time, appid, childID, channelID, tuple2._2);
                statAppChannelIdDeviceActiveDao.saveAppChannelIDMinuteStartUpCount(time, appid, childID, channelID, appChannelID, tuple2._2);
                statPackageIdDeviceActiveDao.savePackageIDMinuteStartUpCount(time, appid, childID, channelID, appChannelID, packageID, tuple2._2);
            }
        });
    }

    private SearchResponse getAdClickInfo(String date,int appid, String idfa){
        Client client = ElasticSearchConfig.getClient();

        String esType = IOSClickIndex + "_" + date;

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("appid", appid))
                .must(QueryBuilders.termQuery("idfa", idfa));

        return client.prepareSearch(IOSClickIndex).setTypes(esType)
                .setQuery(queryBuilder).addSort("ts", SortOrder.ASC).execute().actionGet();
    }

    private SearchResponse getAdClickInfo(String date, int appid, String ip, String deviceName, String osVer) {
        Client client = ElasticSearchConfig.getClient();

        String esType = IOSClickIndex + "_" + date;

        String newOSVer = osVer.replace(".", "_");
        String device =  deviceName.replaceAll("[^a-zA-Z]", "");

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.termQuery("appid", appid))
                .must(QueryBuilders.matchQuery("user_agent" , device))
                .must(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("user_agent", osVer))
                        .should(QueryBuilders.matchQuery("user_agent", newOSVer)));

        return client.prepareSearch(IOSClickIndex).setTypes(esType)
                .setQuery(queryBuilder).addSort("ts", SortOrder.ASC).execute().actionGet();
    }

    public static void main(String[] args) throws Exception {
        //这里测试环境为windows，本地运行
//        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example").setMaster("local");
//
////        conf.set("spark.executor.extraJavaOptions" , "-Dlogback.configurationFile=/opt/yxtest/logback.xml");
//
////        JavaSparkContext sc = new JavaSparkContext(conf);


        String str = "{\"device_os_ver\":\"iOS 11.4.1\",\"imei\":\"68e6f348f50004722c57c474XXXXXXXXX\",\"logid\":\"b6394bbdf6feaf2beb9403904f15cf87\",\"api_ver\":\"1.2.0\",\"app_ver\":\"1.0.6\",\"mac\":\"\",\"package_id\":1,\"child_id\":1,\"device_name\":\"iPhone9,1\",\"app_channel_id\":1,\"os\":2,\"sdk_ver\":\"1.2.0\",\"channel_id\":1,\"appid\":10014,\"idfa\":\"\",\"client_ip\":\"113.5.4.99\",\"ts\":1540783006}";

        JSONObject json = JSONObject.fromObject(str);


        for (int i = 0; i < 10000; i++) {

            deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json, 10014);
        }

    }
}
