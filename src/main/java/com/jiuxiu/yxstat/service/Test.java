package com.jiuxiu.yxstat.service;

import com.jiuxiu.yxstat.es.DeviceInstall;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Created with IDEA by Zhoufy on 2018/5/17.
 *
 * @author Zhoufy
 */
public class Test implements Serializable {

    private static  Logger log = LoggerFactory.getLogger(Test.class);
    private DeviceInstall deviceInstall = DeviceInstall.getDeviceInstall();

    private void init(){
        String top = "device_install";
        String offset0 = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, top + "_0");
        String offset1 = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, top + "_1");

        System.out.print("offset0 :" + offset0 + " ,  offset1 :" +offset1);
        System.out.println();
        if (StringUtils.isEmpty(offset0)) {
            offset0 = "95";
        }

        if(StringUtils.isEmpty(offset1)){
            offset1 = "1";
        }

        String sparkMaster = PropertyUtils.getValue("spark.master.url");
        sparkMaster = "local[2]";

        SparkConf conf = new SparkConf().setMaster(sparkMaster).setAppName("ceshi");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>(16);
        kafkaParams.put("bootstrap.servers", "192.168.0.247:9092");
        kafkaParams.put("group.id", "test-consumer-group");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("enable.auto.commit", false);

        TopicPartition topicPartition0 = new TopicPartition(top , 0);
        TopicPartition topicPartition1 = new TopicPartition(top , 1);

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(top.split(",")));

        Map<TopicPartition,Long> map = new HashMap<>(4);
        map.put(topicPartition0 , Long.valueOf(offset0));
        map.put(topicPartition1 , Long.valueOf(offset1));

        JavaDStream<ConsumerRecord<String,String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams , map)
        );

        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecord) throws Exception {

                JavaRDD<JSONObject> javaRdd = consumerRecord.flatMap(new FlatMapFunction<ConsumerRecord<String,String>, JSONObject>() {
                    @Override
                    public Iterator<JSONObject> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                        System.out.println("收到的数据:" + consumerRecord.value());
                        List<JSONObject> list = new ArrayList<>();
                        list.add(JSONObject.fromObject(consumerRecord.value()));
                        return list.iterator();
                    }
                });

                /**
                 *   android
                 */
                javaRdd.filter(new Function<JSONObject, Boolean>() {
                    @Override
                    public Boolean call(JSONObject jsonObject) throws Exception {
                        return 1 == jsonObject.getInt("os");
                    }
                }).foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) throws Exception {
                        String imei = json.getString("imei");
                        GetResponse response = deviceInstall.getDeviceInstallByID(imei);
                        if (response == null || response.getSource() == null) {
                            deviceInstall.saveDeviceInstall(json);
                        }
                    }
                });


                javaRdd.filter(new Function<JSONObject, Boolean>() {
                    Map<String, JSONObject> iosMap = new HashMap<>(16);
                    @Override
                    public Boolean call(JSONObject json) throws Exception {

                        String imei = json.getString("imei");
                        if (StringUtils.isEmpty(imei)) {
                            imei = json.getString("idfa");
                        }

                        if (iosMap.get(imei) == null && 2 == json.getInt("os")) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).foreach(new VoidFunction<JSONObject>() {
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
                                        flag = true;
                                    }
                                }
                            } else {
                                response = deviceInstall.getDeviceInstallByID(idfa);
                            }
                        } else {
                            response = deviceInstall.getDeviceInstallByID(imei);
                        }

                        if (response == null || response.getSource().isEmpty()) {
                            flag = true;
                        }
                        if (flag) {
                            deviceInstall.saveDeviceInstall(json);
                        }
                    }
                });

                OffsetRange[] offsetRange = ((HasOffsetRanges) consumerRecord.rdd()).offsetRanges();
                for (int i = 0; i < offsetRange.length; i++) {
                    System.out.println("输出：" + i + " ----- " + offsetRange[i]);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, top + "_"+offsetRange[i].partition() , String.valueOf(offsetRange[i].fromOffset()), 0);
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

    public static void main(String[] args) {

        Test test = new Test();
        test.init();
    }
}
