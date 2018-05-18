package com.jiuxiu.yxstat.service;

import com.jiuxiu.yxstat.dao.StatDao;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class ActivateServiceBak implements Serializable{


    private Logger log = LoggerFactory.getLogger(this.getClass());

    StatDao statDao = StatDao.getStatDao();

    public void initialization(){


        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");
        sparkMasterURL = "local[1]";
        String appName = "ceShi";
        int seconds = 10;

        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL , appName , seconds);

        String zkQuorum = PropertyUtils.getValue("kafka.bootstrap.servers");
        String group = PropertyUtils.getValue("kafka.group.id");
        String topics = PropertyUtils.getValue("kafka.topics");

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));


        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc);


        JavaDStream<JSONObject> javaDStream = stream.flatMap(new FlatMapFunction<ConsumerRecord<String,String>, JSONObject>() {

            @Override
            public Iterator<JSONObject> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                log.info("读取到的数据为：" + consumerRecord.value());
                List<JSONObject> list = new ArrayList<>(10);
                JSONObject json = JSONObject.fromObject(consumerRecord.value());
                String imei;

                if(json.has("logid")){
                    imei = json.getString("logid");
                }else{
                    imei = json.getString("sign");
                }

                int appid = json.getInt("appid");
                String redisKey = imei + appid;

                //  logID(kafka消息唯一值，确定消息唯一性)#appid#马甲包id#通道id#子渠道id
                String redisValue = new StringBuffer(imei).append("#")
                        .append(appid).append("#").append(json.getInt("child_id")).append("#").append(json.getInt("channel_id"))
                        .append("#").append(json.getInt("app_channel_id")).toString();

                statDao.saveDeviceInstall(new Object[]{imei , json.getInt("os")});

                JedisUtils.setNotExists(JedisPoolConfigInfo.statRedisPoolKey , redisKey , redisValue , ServiceConstant.REDIS_EXPIRE_TIME_YEARS);
                list.add(json);
                return list.iterator();
            }
        });


        /*
         *  android 数据
         */
        JavaDStream<JSONObject> android = javaDStream.filter(new Function<JSONObject, Boolean>() {

            Map<String,JSONObject>  androidMap = new HashMap<>(16);

            @Override
            public Boolean call(JSONObject v1) throws Exception {
                String logId ;
                if(v1.has("logid")){
                    logId = v1.getString("logid");
                }else{
                    logId = v1.getString("sign");
                }
                if(androidMap.get(logId)==null){
                    androidMap.put(logId , v1);
                    boolean flag = 1 == v1.getInt("os");
                    return flag;
                }else{
                    return false;
                }
            }
        });


        /**
         *   计算android  app 点击次数
         */
        JavaPairDStream<Integer , Integer> androidAppClick =  clickCount(android);

        /**
         *  将点击次数更新数据库
         */
        androidAppClick.foreachRDD(new VoidFunction<JavaPairRDD<Integer, Integer>>() {
            @Override
            public void call(JavaPairRDD<Integer, Integer> stringIntegerJavaPairRDD) throws Exception {
                stringIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                        log.info("````````````````````````````````" + tuple2._1 + "--------------------------" + tuple2._2);
                        updateClickCount(tuple2 , 1);
                    }
                });
            }
        });


        /*
         *   ios 数据
         */
        JavaDStream<JSONObject> ios = javaDStream.filter(new Function<JSONObject, Boolean>() {

            Map<String,JSONObject> iosMap = new HashMap<>(16);

            @Override
            public Boolean call(JSONObject jsonObject) throws Exception {
                String logId ;
                if(jsonObject.has("logid")){
                    logId = jsonObject.getString("logid");
                }else{
                    logId = jsonObject.getString("sign");
                }
                if(iosMap.get(logId)==null){
                    iosMap.put(logId , jsonObject);
                    return 2 == jsonObject.getInt("os");
                }else{
                    return false;
                }
            }
        });


        /**
         *  计算ios app  点击数
         */
        JavaPairDStream<Integer , Integer> iosAppClick = clickCount(ios);


        /**
         *  将点击次数更新数据库
         */
        iosAppClick.foreachRDD(new VoidFunction<JavaPairRDD<Integer, Integer>>() {
            @Override
            public void call(JavaPairRDD<Integer, Integer> stringIntegerJavaPairRDD) throws Exception {
                stringIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                        log.info("````````````````````````````````" + tuple2._1 + "--------------------------" + tuple2._2);
                        updateClickCount(tuple2 , 2);
                    }
                });
            }
        });


        android.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            @Override
            public void call(JavaRDD<JSONObject> jsonObjectJavaRDD) throws Exception {

                jsonObjectJavaRDD.foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) throws Exception {

                        int appid = json.getInt("appid");
                        int channel_id = json.getInt("channel_id");

                        String rowKey = new StringBuilder("app").append("_").append(appid).append("_").append(channel_id).toString();

                        int app_channel_id = json.getInt("app_channel_id");

                        String imei = json.getString("imei");
                        int os = json.getInt("os");




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



    public JavaPairDStream<Integer , Integer> clickCount(JavaDStream<JSONObject> javaDStream){

        return javaDStream.flatMap(new FlatMapFunction<JSONObject, Integer>() {
            @Override
            public Iterator<Integer> call(JSONObject jsonObject) throws Exception {
                List<Integer> list = new ArrayList<>();
                list.add(jsonObject.getInt("appid"));
                return list.iterator();
            }
        }).mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
    }





    /**
     *    更新数据库
     * @param tuple2
     * @param os
     */
    public void updateClickCount(Tuple2<Integer , Integer> tuple2 ,  int os){
        Map<String,Object> map = statDao.getAppClickCountByAppIDAndOS(tuple2._1 ,os);
        int count = 0 ;
        String mapKey = "click_count";
        if(map != null && map.get(mapKey)!=null){
            count = new Integer(map.get(mapKey).toString());
        }
        int newClickCount = count + tuple2._2;
        statDao.updateAppClickCount(new Object[]{tuple2._1 , newClickCount , os, newClickCount});
    }


    public static void main(String[] args){
        ActivateServiceBak activateService = new ActivateServiceBak();
        activateService.initialization();
    }
}
