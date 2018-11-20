package com.nextjoy;

import com.nextjoy.es.ElasticSearchConfig;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created with IDEA by hadoop on 2018/10/30.
 *
 * @author Zhoufy
 */
public class RetentionTest {


    public static void main(String[] args) {
        Client client = ElasticSearchConfig.getClient();

        String index = "device_install";
        String esType = index + "_APPID_10014";

        QueryBuilder queryBuilder = QueryBuilders.boolQuery();


        SearchResponse searchResponse = client.prepareSearch(index).setTypes(esType)
                .setQuery(queryBuilder).execute().actionGet();


        System.out.println(searchResponse.getHits().getHits().length);


    }
}
