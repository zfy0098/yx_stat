package com.jiuxiu.yxstat.service;

import com.jiuxiu.yxstat.es.DeviceInstall;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class ActivateService implements Serializable {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private DeviceInstall deviceInstall = DeviceInstall.getDeviceInstall();

    private void initialization() {
        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");

        sparkMasterURL = "local[2]";
        String appName = "ceShi";
        int seconds = 10;

        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL, appName, seconds);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc);

        JavaDStream<JSONObject> javaDStream = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, JSONObject>() {
            @Override
            public Iterator<JSONObject> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                log.info("读取到的数据为：" + consumerRecord.value());
                List<JSONObject> list = new ArrayList<>(10);
                JSONObject json = JSONObject.fromObject(consumerRecord.value());
                list.add(json);
                return list.iterator();
            }
        });

        /*
         *  android 数据
         */
        JavaDStream<JSONObject> android = javaDStream.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> androidMap = new HashMap<>(16);

            @Override
            public Boolean call(JSONObject v1) throws Exception {
                String imei = v1.getString("imei");
                if (androidMap.get(imei) == null) {
                    androidMap.put(imei, v1);
                    return 1 == v1.getInt("os");
                } else {
                    return false;
                }
            }
        });

        /*
         *    安卓设备激活
         */
        android.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            @Override
            public void call(JavaRDD<JSONObject> jsonObjectJavaRDD) throws Exception {
                jsonObjectJavaRDD.foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) throws Exception {
                        String imei = json.getString("imei");
                        //  设备首次激活
                        GetResponse response = deviceInstall.getDeviceInstallByID(imei);
                        if (response == null || response.getSource() == null) {
                            deviceInstall.saveDeviceInstall(json);
                        }else{
                            // 设备已经激活 但是广告没有激活  保存一条广告的记录
                            response = deviceInstall.getDeviceInstallForAppIDByID(imei , json.getInt("appid"));
                            if(response == null || response.getSource() == null){
                                deviceInstall.saveDeviceInstallForAppid(json);
                            }
                        }
                    }
                });
            }
        });


        /*
         *   ios 数据
         */
        JavaDStream<JSONObject> ios = javaDStream.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> iosMap = new HashMap<>(16);
            @Override
            public Boolean call(JSONObject json) throws Exception {
                String imei = json.getString("imei");
                if (StringUtils.isEmpty(imei)) {
                    imei = json.getString("idfa");
                }
                if (iosMap.get(imei) == null) {
                    return 2 == json.getInt("os");
                } else {
                    return false;
                }
            }
        });

        ios.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            @Override
            public void call(JavaRDD<JSONObject> javaRDD) throws Exception {

                javaRDD.foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) throws Exception {
                        String imei = json.getString("imei");
                        String idfa = json.getString("idfa");
                        boolean flag = false;
                        GetResponse response = null;

                        if (StringUtils.isEmpty(imei)) {
                            if (StringUtils.isEmpty(idfa)) {
                                SearchResponse searchResponse = deviceInstall.iosSpecialSearch(json.getString("client_ip"), json.getString("device_name"), json.getString("device_os_ver"));
                                if (searchResponse != null && searchResponse.getHits() != null) {
                                    SearchHit[] searchHits = searchResponse.getHits().getHits();
                                    if (searchHits.length < 5) {
                                        System.out.println("同ID同系统名，同设备系统版本号 小于5条");
                                        flag = true;
                                    } else {
                                        System.out.println("激活的设备大于5个");
                                    }
                                }
                            } else {
                                response = deviceInstall.getDeviceInstallByID(idfa);
                            }
                        } else {
                            response = deviceInstall.getDeviceInstallByID(imei);
                        }

                        if (response == null || response.getSource() == null) {
                            flag = true;
                        }
                        if (flag) {
                            deviceInstall.saveDeviceInstall(json);
                        }
                    }
                });
            }
        });
        SparkConfiguration.commitOffset(stream);
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ActivateService activateService = new ActivateService();
        activateService.initialization();
    }
}
