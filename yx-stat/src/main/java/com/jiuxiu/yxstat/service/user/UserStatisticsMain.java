package com.jiuxiu.yxstat.service.user;

import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/11.
 *
 * @author ZhouFy
 */
public class UserStatisticsMain implements Serializable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private void userStatisticsData() {

        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");
        String appName = "user_register_login";
        int seconds = 10;
        String topics = PropertyUtils.getValue("kafka.topic.user.name");

        //  确定kafka分区数量
        int partitionCount = Integer.parseInt(PropertyUtils.getValue("user.register.login.partition.count"));

        //  设置没个分区的 offset 信息，从redis中获取保存的offset 如果获取失败 将默认为0
        Map<TopicPartition, Long> topicPartitionMap = new HashMap<>(8);
        long offset = 0;
        for (int i = 0; i < partitionCount; i++) {
            try {
                offset = Long.parseLong(JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + i));
            } catch (NumberFormatException e) {
                log.error("kafka offset 转换异常：" + e.getMessage(), e);
                System.exit(1);
            }
            topicPartitionMap.put(new TopicPartition(topics, i), offset);
        }

        //  创建streaming 对象
        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL, appName, seconds);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc, topicPartitionMap);

        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumer) {


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

                long count = javaRDD.count();
                if(count > 0){
                    Thread userData = new Thread(new UserStatisticsDataService(javaRDD));
                    userData.start();

                    Thread platformTask = new Thread(new PlatformUserStatisticsDataService(javaRDD));
                    platformTask.start();

                    consumer.foreach(x -> {
                        log.info(x.key() + "---------------" + x.value());
                    });
                }

                //遍历分区信息,将新的offset保存到redis中
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumer.rdd()).offsetRanges();
                for (OffsetRange offsetRange : offsetRanges) {
                    log.info("输出 offsetRanges ：" + offsetRange);
                    JedisUtils.set(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + offsetRange.partition() , String.valueOf(offsetRange.fromOffset()), 0);
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
        UserStatisticsMain userStatisticsMain = new UserStatisticsMain();
        userStatisticsMain.userStatisticsData();
    }
}
