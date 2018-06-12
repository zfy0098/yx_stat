package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.StatDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisDeviceActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.elasticsearch.action.get.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @author admin
 */
public class ActivationDataService implements Serializable {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();

    private StatDeviceActiveDao statDeviceActiveDao = StatDeviceActiveDao.getInstance();

    public void initialization(String[] args) {

        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");
        sparkMasterURL = "local[1]";
        String appName = "ceShi";
        int seconds = 10;

        String topics = PropertyUtils.getValue("kafka.topics");

        //  确定kafka分区数量
        int partitionCount = ServiceConstant.DEVICE_INSTALL_PARTITION_COUNT;
        if (args.length > 0) {
            partitionCount = Integer.parseInt(args[0]);
        }

        //  设置没个分区的 offset 信息，从redis中获取保存的offset 如果获取失败 将默认为0
        Map<TopicPartition, Long> topicPartitionMap = new HashMap<>(8);
        for (int i = 0; i < partitionCount; i++) {
            long offset;
            try {
                offset = Long.parseLong(JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + i));
            } catch (NumberFormatException e) {
                offset = 250L;
            }
            topicPartitionMap.put(new TopicPartition(topics, i), offset);
        }

        //  创建streaming 对象
        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL, appName, seconds);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc, topicPartitionMap);


        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumer) throws Exception {

                String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

                JavaRDD<JSONObject> javaRDD = consumer.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, JSONObject>() {
                    @Override
                    public Iterator<JSONObject> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                        List<JSONObject> list = new ArrayList<>();
                        list.add(JSONObject.fromObject(consumerRecord.value()));
                        return list.iterator();
                    }
                });

                //  android 数据
                JavaRDD<JSONObject> android = javaRDD.filter(new Function<JSONObject, Boolean>() {
                    @Override
                    public Boolean call(JSONObject jsonObject) throws Exception {
                        return 1 == jsonObject.getInt("os");
                    }
                });

                // 设置启动次数
                android.foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject jsonObject) throws Exception {
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.ANDROID_STARTUP_COUNT);
                    }
                });
                android.filter(new Function<JSONObject, Boolean>() {
                    Map<String, JSONObject> androidDevice = new HashMap<>(16);

                    @Override
                    public Boolean call(JSONObject json) throws Exception {
                        if (androidDevice.get(json.getString("imei")) == null) {
                            androidDevice.put(json.getString("imei"), json);
                            return true;
                        }
                        return false;
                    }
                }).foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) throws Exception {
                        GetResponse response = deviceInstallInfo.getDeviceInstallByID(json.getString("imei"));
                        if (response == null || response.getSource() == null) {
                            // android 没有激活设备
                            // 将设备信息保存到es中
                            deviceInstallInfo.saveDeviceInstall(json);
                            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.ANDROID_NEW_DEVICE_COUNT);
                        }
                        // 从redis 中获取 设备启动信息 如果不存在 证明当天是 改设备第一次启动 将count 加1 并将信息保存到redis中
                        String key = toDay + JedisDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_INFO + json.getString("imei");
                        String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, key);
                        if (value == null) {
                            JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, key, json.toString(), 0);
                            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_COUNT);
                        }
                    }
                });
                // android 新设备数
                String androidNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.ANDROID_NEW_DEVICE_COUNT);
                //  android 设备启动数
                String androidDeviceStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_COUNT);
                //  android 启动数
                String androidStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.ANDROID_STARTUP_COUNT);

                androidNewDeviceCount = androidNewDeviceCount == null ? "0" : androidNewDeviceCount;
                androidDeviceStartUpCount = androidDeviceStartUpCount == null ? "0" : androidDeviceStartUpCount;
                androidStartUpCount = androidStartUpCount == null ? "0" : androidStartUpCount;


                String zero = "0";
                if (!StringUtils.equals(androidNewDeviceCount, zero) || !StringUtils.equals(androidDeviceStartUpCount, zero) || !StringUtils.equals(androidStartUpCount, zero)) {
                    //  保存数据库操作
                    statDeviceActiveDao.saveDeviceActiveCount(new Object[]{androidNewDeviceCount, androidDeviceStartUpCount, androidStartUpCount, ServiceConstant.ANDROID_OS,
                            androidNewDeviceCount, androidDeviceStartUpCount, androidStartUpCount});
                }


                // ios数据
                JavaRDD<JSONObject> ios = javaRDD.filter(new Function<JSONObject, Boolean>() {
                    @Override
                    public Boolean call(JSONObject json) throws Exception {
                        return 2 == json.getInt("os");
                    }
                });
                // 保存ios 数据启动数
                ios.foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) throws Exception {
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.IOS_STARTUP_COUNT);
                    }
                });
                ios.filter(new Function<JSONObject, Boolean>() {
                    Map<String, JSONObject> iosDevice = new HashMap<>(16);

                    @Override
                    public Boolean call(JSONObject jsonObject) throws Exception {
                        String id = jsonObject.getString("imei");
                        if (StringUtils.isEmpty(id)) {
                            id = jsonObject.getString("idfa");
                        }
                        if (StringUtils.isEmpty(id)) {
                            return true;
                        } else if (iosDevice.get(id) == null) {
                            iosDevice.put(id, jsonObject);
                            return true;
                        }
                        return false;
                    }
                }).foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject jsonObject) throws Exception {

                        String id = jsonObject.getString("imei");
                        if (StringUtils.isEmpty(id)) {
                            id = jsonObject.getString("idfa");
                        }
                        // 没有获取到ios的唯一值
                        if (StringUtils.isEmpty(id)) {

                            String deviceName = jsonObject.getString("device_name");
                            String deviceOSVer = jsonObject.getString("device_os_ver");
                            String ip = jsonObject.getString("client_ip");

                            // no
                            String countKey = JedisDeviceActivationKeyConstant.IOS_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT + ip + ":"+ deviceName + ":" + deviceOSVer;
                            saveIOSNewDeviceInfo(countKey, jsonObject);

                            // 查询设备当天是否有启动记录
                            String key = toDay + JedisDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER + ip + ":"+ deviceName + ":" + deviceOSVer;
                            saveIOSDeviceStartUpInfo(key, jsonObject);

                        } else {
                            GetResponse getResponse = deviceInstallInfo.getDeviceInstallByID(id);
                            if (getResponse == null || getResponse.getSource() == null) {
                                deviceInstallInfo.saveDeviceInstall(jsonObject);
                                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT);
                            }

                            String key = toDay + JedisDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_INFO + id;
                            String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, key);
                            if (value == null) {
                                JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, key, jsonObject.toString(), 0);
                                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT);
                            }
                        }
                    }
                });

                // ios新增设备数
                String iosNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT);
                // ios启动设备数
                String iosDeviceStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT);
                // ios启动次数
                String iosStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceActivationKeyConstant.IOS_STARTUP_COUNT);

                iosNewDeviceCount = iosNewDeviceCount == null ? "0" : iosNewDeviceCount;
                iosDeviceStartUpCount = iosDeviceStartUpCount == null ? "0" : iosDeviceStartUpCount;
                iosStartUpCount = iosStartUpCount == null ? "0" : iosStartUpCount;

                if (!StringUtils.equals(iosNewDeviceCount, zero) || !StringUtils.equals(iosDeviceStartUpCount, zero) || !StringUtils.equals(iosStartUpCount, zero)) {
                    //  保存数据库操作
                    statDeviceActiveDao.saveDeviceActiveCount(new Object[]{iosNewDeviceCount, iosDeviceStartUpCount, iosStartUpCount, ServiceConstant.IOS_OS,
                            iosNewDeviceCount, iosDeviceStartUpCount, iosStartUpCount});
                }

                AndroidActivationDataService.androidActivationData(android);
                IOSActivationDataService.iosActivationData(ios);
                StartupCountDataService.startupCount(javaRDD);

                //遍历分区信息,将新的offset保存到redis中
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumer.rdd()).offsetRanges();
                consumer.foreach(x -> {
                    log.info(x.key() + "---------------" + x.value());
                });
                for (int i = 0; i < offsetRanges.length; i++) {
                    log.info("输出 offsetRanges ：" + offsetRanges[i]);
                    JedisUtils.set(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + offsetRanges[i].partition(), String.valueOf(offsetRanges[i].fromOffset()), 0);
                }
            }
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void saveIOSNewDeviceInfo(String key, JSONObject json) {
        String count = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, key);
        count = count == null ? "0" : count;
        if (Integer.parseInt(count) < ServiceConstant.ACTIVE_COUNT) {
            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, JedisDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT);
            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, key);
            deviceInstallInfo.saveDeviceInstall(json);
        }
    }

    public synchronized void saveIOSDeviceStartUpInfo(String key, JSONObject jsonObject) {
        String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, key);
        value = value == null ? "0" : value;
        if (Integer.parseInt(value) < ServiceConstant.ACTIVE_COUNT) {
            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, key);
            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, JedisDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT);
            String infoKey = JedisDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER
                    + jsonObject.getString("client_ip") + jsonObject.getString("device_name")
                    + jsonObject.getString("device_os_ver") + "_LOGIN:" + jsonObject.getString("logid");
            JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, infoKey, jsonObject.toString(), 0);
        }
    }

    public static void main(String[] args) {
        ActivationDataService activationDataService = new ActivationDataService();
        activationDataService.initialization(args);
    }
}
