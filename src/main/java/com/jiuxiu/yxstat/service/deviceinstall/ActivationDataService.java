package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author admin
 */
public class ActivationDataService implements Serializable {


    private Logger log = LoggerFactory.getLogger(this.getClass());

    private void initialization() {

        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");
        String appName = "device_install";
        int seconds = 10;
        String topics = PropertyUtils.getValue("kafka.topics");

        //  确定kafka分区数量
        int partitionCount = Integer.parseInt(PropertyUtils.getValue("device.install.partition.count"));

        //  设置每个分区的 offset 信息，从redis中获取保存的offset 如果获取失败 将默认为0
        Map<TopicPartition, Long> topicPartitionMap = new HashMap<>(8);
        long offset = 0;
        for (int i = 0; i < partitionCount; i++) {
            try {
                offset = Long.parseLong(JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + i));
                log.info("topics : " + topics + " , get partition ：" + i + " , offset:" + offset);
            } catch (NumberFormatException e) {
                log.info("redis 获取 offset 转化数据异常：" + e.getMessage() , e);
                System.exit(1);
            }
            topicPartitionMap.put(new TopicPartition(topics, i), offset);
        }
        //  创建streaming 对象
        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL, appName, seconds);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc, topicPartitionMap);

        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumer) throws Exception {

                String isExit = JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey , topics );

                if("1".equals(isExit)){
                    log.info("redis 获取 isExit 为：" + isExit + " , 程序退出 ");
                    System.exit(1);
                }


                JavaRDD<JSONObject> javaRDD = consumer.map(new Function<ConsumerRecord<String, String>, JSONObject>() {
                    @Override
                    public JSONObject call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                        return JSONObject.fromObject(consumerRecord.value());
                    }
                });

                //  android 数据
                JavaRDD<JSONObject> android = javaRDD.filter(new Function<JSONObject, Boolean>() {
                    @Override
                    public Boolean call(JSONObject jsonObject) throws Exception {
                        return ServiceConstant.ANDROID_OS == jsonObject.getInt("os");
                    }
                });
                // ios数据
                JavaRDD<JSONObject> ios = javaRDD.filter(new Function<JSONObject, Boolean>() {
                    @Override
                    public Boolean call(JSONObject json) throws Exception {
                        return ServiceConstant.IOS_OS == json.getInt("os");
                    }
                });
                // 计算 启动数 数据
                Thread startupCountTask = new Thread(new StartupCountDataService(javaRDD));
                startupCountTask.start();

                // 平台android数据
                Thread platformAndroidTask = new Thread(new PlatformAndroidActivationDataService(android));
                platformAndroidTask.start();

                // 平台 ios 数据
                Thread platformIOSTask = new Thread(new PlatformIOSActivationDataService(ios));
                platformIOSTask.start();

                // 计算 android 数据
                Thread androidTask = new Thread(new AndroidActivationDataService(android));
                androidTask.start();

                // 计算 ios 数据
                Thread iosTask = new Thread(new IOSActivationDataService(ios));
                iosTask.start();

                consumer.foreach(x -> {
                    System.out.println(x.key() + "----------" + x.value());
                });

                //遍历分区信息,将新的offset保存到redis中
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumer.rdd()).offsetRanges();
                for (OffsetRange offsetRang : offsetRanges) {
                    log.info("输出 offsetRanges ：" + offsetRang);
                    JedisUtils.set(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + offsetRang.partition() , String.valueOf(offsetRang.fromOffset()), 0);
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
        ActivationDataService activationDataService = new ActivationDataService();
        activationDataService.initialization();
    }
}
