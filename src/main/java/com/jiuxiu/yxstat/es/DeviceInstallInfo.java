package com.jiuxiu.yxstat.es;

import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/18.
 *
 * @author Zhoufy
 */
public class DeviceInstallInfo implements Serializable {

    private static DeviceInstallInfo deviceInstall = null;

    private String index = PropertyUtils.getValue("es.device_install_index");

    private DeviceInstallInfo() {
    }

    public static DeviceInstallInfo getInstance() {
        if (deviceInstall == null) {
            deviceInstall = new DeviceInstallInfo();
        }
        return deviceInstall;
    }


    /**
     * 根据imei 查询设备激活情况
     *
     * @param imei
     * @return
     */
    public GetResponse getDeviceInstallByID(String imei) {
        Client client = ElasticearchConfig.getClient();
        GetRequestBuilder getRequestBuilder = client.prepareGet(index, "device_install", imei);
        return getRequestBuilder.execute().actionGet();
    }

    /**
     * ios 特殊数据处理 ， 没有imei 和idfa  通过 ip  devicename  deviceosver 来查询设备激活情况
     *
     * @param ip
     * @param deviceName
     * @param deviceOsVer
     * @return
     */
    public synchronized SearchResponse iosSpecialSearch(String ip, String deviceName, String deviceOsVer) {
        Client client = ElasticearchConfig.getClient();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.termQuery("device_name", deviceName))
                .must(QueryBuilders.termQuery("device_os_ver", deviceOsVer))
                .must(QueryBuilders.termQuery("os", 2))
                .must(QueryBuilders.termQuery("imei", ""))
                .must(QueryBuilders.termQuery("idfa", ""));

        SearchResponse response = null;
        if (client != null) {
            response = client.prepareSearch(index).setTypes("device_install").setQuery(queryBuilder).execute().actionGet();
        }
        return response;
    }


    /**
     * 保存设备激活信息
     *
     * @param json
     */
    public synchronized void saveDeviceInstall(JSONObject json) {
        Client client = ElasticearchConfig.getClient();
        Map<String, Object> source = esDataReduction(json);
        if (client != null) {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, "device_install");
            String id = json.getString("imei");
            if (StringUtils.isEmpty(id)) {
                id = json.getString("idfa");
            }
            if (!StringUtils.isEmpty(id)) {
                indexRequestBuilder.setId(id);
            }
            indexRequestBuilder.setSource(source).execute().actionGet();
        }
    }


    /**
     * 根据 imei 和 5种 id  查询对应的设备激活情况
     *
     * @param imei
     * @param typeId
     * @return
     */
    public GetResponse getDeviceInstallForTypeIDByImei(String imei, String type, int typeId) {
        Client client = ElasticearchConfig.getClient();
        String esType = index + "_" + type + "_" + typeId;
        GetRequestBuilder getRequestBuilder = client.prepareGet(index, esType, imei);
        return getRequestBuilder.execute().actionGet();
    }

    /**
     * @param ip
     * @param deviceName
     * @param deviceOsVer
     * @param typeId
     * @return
     */
    public SearchResponse iosSpecialSearchForTypeId(String ip, String deviceName, String deviceOsVer, String type, int typeId) {
        Client client = ElasticearchConfig.getClient();
        String esType = index + "_" + type + "_" + typeId;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.termQuery("device_name", deviceName))
                .must(QueryBuilders.termQuery("device_os_ver", deviceOsVer))
                .must(QueryBuilders.termQuery("os", 2))
                .must(QueryBuilders.termQuery("imei", ""))
                .must(QueryBuilders.termQuery("idfa", ""));

        SearchResponse response = null;
        if (client != null) {
            response = client.prepareSearch(index).setTypes(esType).setQuery(queryBuilder).execute().actionGet();
        }
        return response;
    }

    /**
     * 保存设备对应 typeid 激活信息
     *
     * @param json
     */
    public void saveDeviceInstallForTypeID(JSONObject json, String type, int typeId) {

        String esType = index + "_" + type + "_" + typeId;
        Client client = ElasticearchConfig.getClient();
        Map<String, Object> source = esDataReduction(json);
        if (client != null) {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, esType);
            String id = json.getString("imei");
            if (StringUtils.isEmpty(id)) {
                id = json.getString("idfa");
            }
            if (!StringUtils.isEmpty(id)) {
                indexRequestBuilder.setId(id);
            }
            indexRequestBuilder.setSource(source).execute().actionGet();
        }
    }

    /**
     * 保存es的数据转换  将 json 的key 转换成 es 对应的 field
     *
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
