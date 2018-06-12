package com.jiuxiu.yxstat.spark;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/15.
 *
 * @author Zhoufy
 */
public class SparkConfiguration {

    private static Logger log = LoggerFactory.getLogger(SparkConfiguration.class);

    public static JavaStreamingContext getJavaStreamingContext(String sparkMasterURL, String appName, int seconds) {
        SparkConf conf = new SparkConf().setMaster(sparkMasterURL).set("spark.executor.cores" , "1").setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        return new JavaStreamingContext(sc, Durations.seconds(seconds));
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> initialization(JavaStreamingContext ssc) {
        String zkQuorum = PropertyUtils.getValue("kafka.bootstrap.servers");
        String group = PropertyUtils.getValue("kafka.group.id");
        String topics = PropertyUtils.getValue("kafka.topics");
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>(16);
        kafkaParams.put("bootstrap.servers", zkQuorum);
        kafkaParams.put("group.id", group);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("enable.auto.commit", false);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );
    }

    /**
     * 自助管理 offset
     *
     * @param ssc
     * @param topicPartitionMap
     * @return
     */
    public static JavaDStream<ConsumerRecord<String, String>> initialization(JavaStreamingContext ssc, Map<TopicPartition, Long> topicPartitionMap) {
        String zkQuorum = PropertyUtils.getValue("kafka.bootstrap.servers");
        String group = PropertyUtils.getValue("kafka.group.id");
        String topics = PropertyUtils.getValue("kafka.topics");
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>(16);
        kafkaParams.put("bootstrap.servers", zkQuorum);
        kafkaParams.put("group.id", group);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("enable.auto.commit", false);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet , kafkaParams, topicPartitionMap)
        );
    }


    public static void commitOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                rdd.foreach(x -> {
                    log.info(x.key() + "---------------" + x.value());
                });
                for (int i = 0; i < offsetRanges.length; i++) {
                    log.info("输出 offsetRanges ：" + offsetRanges[i]);

                }
                ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            }
        });
    }
}
