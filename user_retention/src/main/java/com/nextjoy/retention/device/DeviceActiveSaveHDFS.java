package com.nextjoy.retention.device;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.nextjoy.es.ElasticSearchConfig;
import com.nextjoy.retention.utils.DateUtils;
import com.nextjoy.retention.utils.PropertyUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created with IDEA by hadoop on 2018/10/29.
 *
 * @author Zhoufy
 */
public class DeviceActiveSaveHDFS {

    private static final String INDEX = PropertyUtils.getValue("es.device.install.index");


    private void run(String nowDate) {

        Client client = ElasticSearchConfig.getClient();

        GetMappingsResponse res = null;
        try {
            res = client.admin().indices().getMappings(new GetMappingsRequest().indices(INDEX)).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        String startDate = "";
        List<String> list = new ArrayList<>();
        if (res != null) {
            ImmutableOpenMap<String, MappingMetaData> mapping = res.mappings().get(INDEX);

            long startTime = 0;
            long endTime ;

            try {
                SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd");
                Date date = format.parse(nowDate);
                endTime = date.getTime() / 1000;

                startDate = DateUtils.dateAgo(nowDate , 1);
                date = format.parse(startDate);
                startTime = date.getTime() / 1000;

            } catch (ParseException e) {
                endTime = 0;
            }

            for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
                System.out.println("type = " + c.key);
                System.out.println("columns = " + c.value.source());
                String type = c.key;
                if (type.startsWith("device_install_APPID_")) {
                    getDeviceList(type , startTime, endTime , list);
                }
            }
        }
        System.out.println(list.size());
        // spark://cdh01:7077

        SparkConf conf = new SparkConf().setAppName("DeviceActivateSaveHDFS").setMaster("spark://172.26.110.193:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        if (list.size() > 0) {
            JavaRDD<String> javaRDD = sc.parallelize(list);
            javaRDD.repartition(1).saveAsTextFile("/opt/stat.game.nextjoy.com/deviceactive/" + startDate + "/");
        }
    }




    public void getDeviceList(String type, long startTime, long endTime , List<String> list) {
        Client client = ElasticSearchConfig.getClient();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("install_time").gt(startTime).lt(endTime));

        SearchResponse searchResponse = client.prepareSearch(INDEX)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setSize(10000)
                .setScroll(TimeValue.timeValueMinutes(8))
                .execute().actionGet();
        while(true){
            for (SearchHit hit : searchResponse.getHits()) {
                list.add(hit.getSourceAsString());
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
        }
    }



    public void gettest(){
        Client client = ElasticSearchConfig.getClient();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("_id","AWa_bTptolrOOxD9LNhf"));

        SearchResponse searchResponse = client.prepareSearch(INDEX)
                .setTypes("device_install_APPID_10014")
                .setQuery(queryBuilder)
                .setSize(10000)
                .setScroll(TimeValue.timeValueMinutes(8))
                .execute().actionGet();
        System.out.println(searchResponse.getHits().getHits()[0].getSourceAsString());
    }



    public static void main(String[] args) {

        String nowDate;
        if(args.length > 0){
            nowDate = args[0];
        } else {
            nowDate = DateUtils.getNowDate(DateUtils.YYYY_MM_DD);
        }

        DeviceActiveSaveHDFS deviceActiveSaveHDFS = new DeviceActiveSaveHDFS();
        deviceActiveSaveHDFS.run(nowDate);
    }
}
