package com.jiuxiu.yxstat.spark;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IDEA by Zhoufy on 2018/5/15.
 *
 * @author Zhoufy
 */
public class SparkTest2 implements Serializable {

    public void init() {
        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");

        SparkConf conf = new SparkConf().setMaster("local").setAppName("streaming word count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

        JavaInputDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc);

        JavaDStream<JSONObject> x = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, JSONObject>() {
            @Override
            public Iterator<JSONObject> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                JSONObject json = JSONObject.fromObject(consumerRecord.value());
                List<JSONObject> arr = new ArrayList<JSONObject>();
                arr.add(json);
                return arr.iterator();
            }
        });

        JavaPairDStream<String, Integer> counts = x.flatMap(new FlatMapFunction<JSONObject, Integer>() {
            @Override
            public Iterator<Integer> call(JSONObject jsonObject) throws Exception {
                String count = jsonObject.getString("key");
                int x = Integer.valueOf(count);
                List<Integer> list = new ArrayList<>(2);
                list.add(x);
                return list.iterator();
            }
        }).mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>("key", integer);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        counts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                stringIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        System.out.println("````````````````````````````````" + stringIntegerTuple2._1 + "--------------------------" + stringIntegerTuple2._2);
                    }
                });
            }
        });

        System.out.println(" lines .foreachrdd ===== >");
        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                rdd.foreach(x -> {
                    System.out.println(x.key() + "---------------" + x.value());
                });

                for (int i = 0; i < offsetRanges.length; i++) {
                    System.out.println("输出 offsetRanges ：" + offsetRanges[i]);
                }
                ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
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
        SparkTest2 sparkTest2 = new SparkTest2();
        sparkTest2.init();
    }

}
