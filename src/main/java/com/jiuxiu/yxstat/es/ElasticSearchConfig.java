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
public class ElasticSearchConfig {


    private static Client client = null;
    private final static String CLUSTER_NAME = PropertyUtils.getValue("es.cluster_name");

    private final static String HOST = PropertyUtils.getValue("es.device.install.host");
    private final static int PORT = Integer.parseInt(PropertyUtils.getValue("es.device.install.port"));

    private final static String SECOND_HOST = PropertyUtils.getValue("es.device.install.host.second");
    private final static int SECOND_PORT = Integer.parseInt(PropertyUtils.getValue("es.device.install.port.second"));

    private final static String THIRD_HOST = PropertyUtils.getValue("es.device.install.host.third");
    private final static int THIRD_PORT = Integer.parseInt(PropertyUtils.getValue("es.device.install.port.third"));



    /**
     *   获取client 对象
     * @return
     */
    public static Client getClient(){
       if (client == null){
           try {
               Settings settings = Settings.builder()
                       .put("cluster.name", CLUSTER_NAME)
                       .put("client.transport.sniff" , true)
                       .build();
               client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(SECOND_HOST), SECOND_PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(THIRD_HOST), THIRD_PORT));

               return client;
           } catch (UnknownHostException e) {
               client = null;
           }
       }
       return client;
    }


    /**
     *   关闭client  释放资源
     * @param client
     */
    public static void closeClient(Client client){
        if(client != null){
//            client.close();
        }
    }
}
