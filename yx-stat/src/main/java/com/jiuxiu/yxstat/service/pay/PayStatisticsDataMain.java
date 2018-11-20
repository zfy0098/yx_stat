package com.jiuxiu.yxstat.service.pay;

import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/7/6.
 *
 * @author ZhouFy
 */
public class PayStatisticsDataMain implements Serializable{

    private PayStatisticsDataService payStatisticsDataService = PayStatisticsDataService.getInstance();

    private PlatformPayOrderDataService platformPayOrderDataService = PlatformPayOrderDataService.getInstance();

    private void payStatisticsData(){
        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");
        String appName = "pay_order";
        int seconds = 10;
        String topics = PropertyUtils.getValue("kafka.topic.pay.name");
        //  确定kafka分区数量
        int partitionCount = Integer.parseInt(PropertyUtils.getValue("pay.order.partition.count"));
        //  设置没个分区的 offset 信息，从redis中获取保存的offset 如果获取失败 将默认为0
        Map<TopicPartition, Long> topicPartitionMap = new HashMap<>(8);
        long offset = 0;
        for (int i = 0; i < partitionCount; i++) {
            try {
                offset = Long.parseLong(JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + i));
            } catch (NumberFormatException e) {
                System.out.println("kafka offset 转换异常：" + e.getMessage());
//                System.exit(1);
                offset = 0;
            }
            topicPartitionMap.put(new TopicPartition(topics, i), offset);
        }

        //  创建streaming 对象
        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL, appName, seconds);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc, topicPartitionMap);

        stream.foreachRDD(consumer -> {
            JavaRDD<JSONObject> javaRDD = consumer.map(consumerRecord -> JSONObject.fromObject(consumerRecord.value()));
            String isExit = JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey , topics);
            if("1".equals(isExit)){
                System.out.println("redis 中获取 退出标识为【 1 】 , 程序退出. ");
                System.exit(1);
            }
            long count = javaRDD.count();
            if(count > 0){

                JavaRDD<JSONObject> payOrder = javaRDD.filter(jsonObject ->
                     1 == jsonObject.getInt("order_type")
                );

                payStatisticsDataService.payOrderData(payOrder);
                platformPayOrderDataService.platformPayOrderData(payOrder);

                consumer.foreach(x ->
                        System.out.println("pay_order_message:offset:" + x.offset() + ",partition:" + x.partition() + ",value=" + x.value())
                );
            }
            //遍历分区信息,将新的offset保存到redis中
            for (OffsetRange offsetRange : ((HasOffsetRanges) consumer.rdd()).offsetRanges()) {
                System.out.println("输出 offsetRanges ：" + offsetRange);
                JedisUtils.set(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + offsetRange.partition(), String.valueOf(offsetRange.fromOffset()), 0);
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
        PayStatisticsDataMain payStatisticsDataMain = new PayStatisticsDataMain();
        payStatisticsDataMain.payStatisticsData();
    }
}
