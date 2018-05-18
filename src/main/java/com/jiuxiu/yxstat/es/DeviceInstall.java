package com.jiuxiu.yxstat.es;

import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import sun.tools.jar.CommandLine;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/18.
 *
 * @author Zhoufy
 */
public class DeviceInstall implements Serializable {


    private static DeviceInstall deviceInstall = null;

    private DeviceInstall() {
    }

    public static DeviceInstall getDeviceInstall() {
        if (deviceInstall == null) {
            return new DeviceInstall();
        }
        return deviceInstall;
    }


    /**
     *    根据imei 查询设备激活情况
     * @param imei
     * @return
     */
    public GetResponse getDeviceInstallByID(String imei) {
        Client client = ElasticearchConfig.getClient();
        GetRequestBuilder getRequestBuilder = client.prepareGet("device_install", "device_install", imei);
        GetResponse getResponse =  getRequestBuilder.execute().actionGet();
        ElasticearchConfig.closeClient(client);
        return getResponse;
    }


    /**
     *   根据 imei 和appid 查询设备对应的 appid 激活情况
     * @param imei
     * @param appid
     * @return
     */
    public GetResponse getDeviceInstallForAppIDByID(String imei , int appid){
        Client client = ElasticearchConfig.getClient();

        String index = "device_install";
        String type = index + appid;

        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type , imei);
        GetResponse getResponse =  getRequestBuilder.execute().actionGet();
        ElasticearchConfig.closeClient(client);
        return getResponse;
    }


    /**
     *  保存设备激活信息
     * @param json
     */
    public void saveDeviceInstall(JSONObject json) {
        Client client = ElasticearchConfig.getClient();
        Map<String, Object> source = esDataReduction(json);
        if (client != null) {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("device_install", "device_install");
            // 如果是android系统 直接保存imei
            if(json.getInt("os") == 1){
                indexRequestBuilder.setId(json.getString("imei"));
            }else if(json.getInt("os") == 2){
                //  如果是ios系统 判断imei 如果不存在 保存idfa 如果不存在 不手动生成idfa
                if(!StringUtils.isEmpty(json.getString("imei"))){
                    indexRequestBuilder.setId(json.getString("imei"));
                }else if(!StringUtils.isEmpty(json.getString("idfa"))){
                    indexRequestBuilder.setId(json.getString("idfa"));
                }
            }
            indexRequestBuilder.setSource(source).execute().actionGet();
        }
        ElasticearchConfig.closeClient(client);

        // 设备如果如果没有激活过 证明对应广告也没有激活
        saveDeviceInstallForAppid(json);
    }


    /**
     *   保存设备对应 appid 激活信息
     * @param json
     */
    public void saveDeviceInstallForAppid(JSONObject json){

        String index = "device_install";
        String type = index +  json.getInt("appid");

        Client client = ElasticearchConfig.getClient();
        Map<String, Object> source = esDataReduction(json);
        if (client != null) {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type);
            // 如果是android系统 直接保存imei
            if(json.getInt("os") == 1){
                indexRequestBuilder.setId(json.getString("imei"));
            }else if(json.getInt("os") == 2){
                //  如果是ios系统 判断imei 如果不存在 保存idfa 如果不存在 不手动生成idfa
                if(!StringUtils.isEmpty(json.getString("imei"))){
                    indexRequestBuilder.setId(json.getString("imei"));
                }else if(!StringUtils.isEmpty(json.getString("idfa"))){
                    indexRequestBuilder.setId(json.getString("idfa"));
                }
            }
            indexRequestBuilder.setSource(source).execute().actionGet();
        }
        ElasticearchConfig.closeClient(client);
    }


    /**
     *   ios 特殊数据处理 ， 没有imei 和idfa 的情况
     * @param ip
     * @param device_name
     * @param device_os_ver
     * @return
     */
    public SearchResponse iosSpecialSearch(String ip , String device_name ,String  device_os_ver){
        Client client = ElasticearchConfig.getClient();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.termQuery("device_name" , device_name))
                .must(QueryBuilders.termQuery("device_os_ver" , device_os_ver));

        SearchResponse response2 = null;
        if(client!=null){
            response2 = client.prepareSearch("device_install" , "device_install").setQuery(queryBuilder).execute().actionGet();
        }
        ElasticearchConfig.closeClient(client);
        return response2;

    }


    /**
     *   保存es的数据转换  讲json 的key 转换成 es 对应的 field
     * @param json
     * @return
     */
    private Map<String, Object> esDataReduction(JSONObject json) {
        Map<String, Object> source = new HashMap<>(16);
        source.put("app_channel_id", json.getInt("child_id"));
        source.put("appid", json.getInt("appid"));
        source.put("channel_id", json.getInt("channel_id"));
        source.put("child_id", json.getInt("child_id"));
        source.put("device_name", json.getString("device_name"));
        source.put("device_os_ver", json.getString("device_os_ver"));
        source.put("idfa", json.getString("idfa"));
        source.put("imei", json.getString("imei"));
        source.put("install_time", System.currentTimeMillis() / 1000);
        source.put("ip", json.getString("client_ip"));
        source.put("mac", json.getString("mac"));
        source.put("os", json.getInt("os"));
        source.put("package_id", json.getInt("package_id"));
        return source;
    }
}
