package com.jiuxiu.yxstat;

import com.jiuxiu.yxstat.es.ElasticSearchConfig;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.utils.MD5Utils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class Tester {


    private SearchResponse getAndroidClickInfo(String date, int appid, int childID, int channelID, int appChannelID, int packageID, String ip, String deviceName, String osVer) {
        Client client = ElasticSearchConfig.getClient();

        String esType = "android_click_20181109";

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.termQuery("appid", appid))
                .must(QueryBuilders.termQuery("child_id", childID))
                .must(QueryBuilders.termQuery("channel_id", channelID))
                .must(QueryBuilders.termQuery("app_channel_id" , appChannelID))
                .must(QueryBuilders.termQuery("package_id", packageID))
                .must(QueryBuilders.termQuery("status" , 0))
                .must(QueryBuilders.matchQuery("user_agent", deviceName))
                .must(QueryBuilders.matchQuery("user_agent", osVer));

        return client.prepareSearch("android_click").setTypes(esType)
                .setQuery(queryBuilder).addSort("ts", SortOrder.ASC).execute().actionGet();
    }


    public void init() throws Exception {


        String[] imeis = {"862629036534629",
                "864315032705896",
                "864618035306517",
                "865305031552828",
                "865959046596806",
                "865968034272551",
                "866215034842481",
                "866432032166249",
                "867008035910277",
                "867180035749847",
                "867271031152142",
                "868080044957937",
                "869379036672580",
                "87f3d453fa2cb68a"};


        for (String imei: imeis) {
            System.out.println(imei + "------" + MD5Utils.getMD5(imei));
        }


    }

    public static void main(String[] args) throws Exception {


        Tester t = new Tester();
        t.init();


    }

}
