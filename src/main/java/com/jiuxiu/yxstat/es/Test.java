package com.jiuxiu.yxstat.es;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;

/**
 * Created with IDEA by Zhoufy on 2018/5/18.
 *
 * @author Zhoufy
 */
public class Test {


    public static void main(String[] args){


        Client client = ElasticearchConfig.getClient();

        if(client == null){
            return ;
        }

        GetRequestBuilder getRequestBuilder = client.prepareGet("device_install" , "app_install" , "xxxxxx");
        GetResponse response = getRequestBuilder.execute().actionGet();

        System.out.println(response.getSource().toString());

        ElasticearchConfig.closeClient(client);
    }
}
