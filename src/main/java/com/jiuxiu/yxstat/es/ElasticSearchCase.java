package com.jiuxiu.yxstat.es;

import com.jiuxiu.yxstat.es.bean.UserBean;
import net.sf.json.JSONObject;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class ElasticSearchCase {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    public void init() {

        Client client = ElasticearchConfig.getClient();

        if(client == null){
            return ;
        }

        GetRequestBuilder getRequestBuilder = client.prepareGet("user_index" , "user" , "31289801");
        GetResponse response = getRequestBuilder.execute().actionGet();

        UserBean userBean = (UserBean) JSONObject.toBean(JSONObject.fromObject(response.getSource()), UserBean.class);

        log.info(userBean.getAccountid());
        log.info(userBean.getUid() + "");


        log.info("------------------------------------------------------------------------------------------------------");


        log.info(response.toString());

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("registchannel", "8169live"))
                .must(QueryBuilders.rangeQuery("registtime").gt(1505900222).lt(1505900224));


        SearchResponse response2 = client.prepareSearch("user_index" , "user").setQuery(queryBuilder)
                .setFrom(0).setSize(10)
                .execute().actionGet();

        SearchHit[] searchHits = response2.getHits().getHits();
        for (int i = 0; i < searchHits.length; i++) {
            SearchHit searchHit = searchHits[i];
            log.info(searchHit.getSourceAsString());
        }


        SearchResponse response1 = client.prepareSearch("room_index_v2" , "room").execute().actionGet();
        log.info(response1.toString());



        ElasticearchConfig.closeClient(client);
    }

    public static void main(String[] args){
        ElasticSearchCase elasticSearchTest = new ElasticSearchCase();
        elasticSearchTest.init();
    }


}
