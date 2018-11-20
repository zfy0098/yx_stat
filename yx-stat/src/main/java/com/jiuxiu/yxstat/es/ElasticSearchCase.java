package com.jiuxiu.yxstat.es;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class ElasticSearchCase {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    public void init() {

        Client client = ElasticSearchConfig.getClient();

        if(client == null){
            return ;
        }

        //7dde6bd71c1807854164982f21635973

        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index("android_click");
            updateRequest.type("android_click_20181110");
            updateRequest.id("AWbxX5pKfXyT0_zQrrQT");
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("mac", "updatealtermac")
                    .endObject());


            client.update(updateRequest).get();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


        ElasticSearchConfig.closeClient(client);
    }

    public static void main(String[] args){
        ElasticSearchCase elasticSearchTest = new ElasticSearchCase();
        elasticSearchTest.init();
    }


}
