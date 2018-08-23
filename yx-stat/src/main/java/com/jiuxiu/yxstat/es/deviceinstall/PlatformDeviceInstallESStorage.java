package com.jiuxiu.yxstat.es.deviceinstall;

import com.jiuxiu.yxstat.es.ElasticSearchConfig;
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
public class PlatformDeviceInstallESStorage implements Serializable {

    private static PlatformDeviceInstallESStorage deviceInstall = null;

    private String index = PropertyUtils.getValue("es.device.install.index");

    private PlatformDeviceInstallESStorage() {
    }

    public static PlatformDeviceInstallESStorage getInstance() {
        if (deviceInstall == null) {
            deviceInstall = new PlatformDeviceInstallESStorage();
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
        Client client = ElasticSearchConfig.getClient();
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
        Client client = ElasticSearchConfig.getClient();

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
        Client client = ElasticSearchConfig.getClient();
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
     * 保存es的数据转换  将 json 的key 转换成 es 对应的 field
     *
     * @param json
     * @return
     */
    private Map<String, Object> esDataReduction(JSONObject json) {
        Map<String, Object> source = new HashMap<>(16);
        source.put("app_channel_id", json.getInt("app_channel_id"));
        source.put("appid", json.getInt("appid"));
        source.put("channel_id", json.getInt("channel_id"));
        source.put("child_id", json.getInt("child_id"));
        source.put("device_name", json.getString("device_name"));
        source.put("device_os_ver", json.getString("device_os_ver"));
        source.put("idfa", json.getString("idfa"));
        source.put("imei", json.getString("imei"));
        source.put("install_time", json.getLong("ts"));
        source.put("ip", json.getString("client_ip"));
        source.put("mac", json.getString("mac"));
        source.put("os", json.getInt("os"));
        source.put("package_id", json.getInt("package_id"));
        return source;
    }
}
