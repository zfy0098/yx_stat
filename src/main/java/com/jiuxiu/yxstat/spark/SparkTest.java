package com.jiuxiu.yxstat.spark;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/5/10.
 *
 * @author Zhoufy
 */
public class SparkTest {

    private static Logger log = LoggerFactory.getLogger(SparkTest.class);


    public static void main(String[] args) throws  InterruptedException {


        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext("sparMaterURL" , "appName" , 10);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<String, String>> lines = SparkConfiguration.initialization(ssc);

        log.info("lines.flatMap ============>");
        System.out.println("lines.flatMap ============>");

        JavaPairDStream<String, Integer> counts = lines.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {

            @Override
                public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {

                log.info("获取  offset : " + stringStringConsumerRecord.offset());

                return Arrays.asList(stringStringConsumerRecord.value().split("#")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });


        log.info("count.foreachRDD ===== >");
        System.out.println("count.foreachRDD ===== >");

        counts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                stringIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        log.info("````````````````````````````````" + stringIntegerTuple2._1 + "--------------------------" + stringIntegerTuple2._2);
                        System.out.println("````````````````````````````````" + stringIntegerTuple2._1 + "--------------------------" + stringIntegerTuple2._2);
                    }
                });
            }
        });

        log.info(" lines .foreachrdd ===== >");
        System.out.println(" lines .foreachrdd ===== >");
        lines.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                rdd.foreach(x -> {
                    System.out.println(x.key() + "---------------" + x.value());
                });


                rdd.foreach(new VoidFunction<ConsumerRecord<String,String>>(){
                    @Override
                    public void call(ConsumerRecord<String,String> consumerRecord){
                        System.out.println(consumerRecord.key() + "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" + consumerRecord.value());
                    }
                } );


                for (int i = 0; i < offsetRanges.length; i++) {
                    System.out.println("输出 offsetRanges ：" + offsetRanges[i]);
                }
                ((CanCommitOffsets) lines.inputDStream()).commitAsync(offsetRanges);
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }


}
