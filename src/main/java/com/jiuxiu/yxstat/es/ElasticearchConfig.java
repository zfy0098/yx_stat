package com.jiuxiu.yxstat.es;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created with IDEA by Zhoufy on 2018/5/15.
 *
 * @author Zhoufy
 */
public class ElasticearchConfig {


    private static Client client;
    private final static String CLUSTER_NAME = PropertyUtils.getValue("es.cluster_name");
    private final static String HOST = PropertyUtils.getValue("es.host");
    private final static int PORT = Integer.parseInt(PropertyUtils.getValue("es.port"));


    /**
     *   获取client 对象
     * @return
     */
    public static Client getClient(){
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", CLUSTER_NAME)
                    .build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
            return client;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     *   关闭client  释放资源
     * @param client
     */
    public static void closeClient(Client client){
        if(client != null){
            client.close();
        }
    }
}
