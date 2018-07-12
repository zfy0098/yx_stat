package com.jiuxiu.yxstat.es.deviceinstall;

import com.jiuxiu.yxstat.es.ElasticSearchConfig;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/12.
 *
 * @author ZhouFy
 */
public class DeviceActivationStatisticsESStorage implements Serializable {

    private String index = PropertyUtils.getValue("es.device.install.index");

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = new DeviceActivationStatisticsESStorage();

    private DeviceActivationStatisticsESStorage() {
    }

    public synchronized static DeviceActivationStatisticsESStorage getInstance() {
        return deviceActivationStatisticsESStorage;
    }

    /**
     * 查询设备激活信息
     *
     * @param imei
     * @param appid
     * @param childID
     * @return
     */
    public SearchResponse getAppDeviceActivationForImei(String imei, int appid, int childID) {
        Client client = ElasticSearchConfig.getClient();
        String type = index + "_APPID_" + appid;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("imei", imei))
                .must(QueryBuilders.termQuery("child_id", childID));
        return client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();
    }

    /**
     * 查询 设备激活信息 idfa 为查询条件
     *
     * @param idfa
     * @param appid
     * @param childID
     * @return
     */
    public SearchResponse getAppDeviceActivationForIdfa(String idfa, int appid, int childID) {
        Client client = ElasticSearchConfig.getClient();
        String type = index + "_APPID_" + appid;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("idfa", idfa))
                .must(QueryBuilders.termQuery("child_id", childID));
        return client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();
    }


    /**
     * @param ip          访问ip
     * @param deviceName  设备名称
     * @param deviceOsVer 设备版本号
     * @return
     */
    public SearchResponse iosSpecialSearchForAppID(String ip, String deviceName, String deviceOsVer, int appid, int childID) {
        Client client = ElasticSearchConfig.getClient();
        String esType = index + "_APPID_" + appid;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.termQuery("device_name", deviceName))
                .must(QueryBuilders.termQuery("device_os_ver", deviceOsVer))
                .must(QueryBuilders.termQuery("child_id", childID))
                .must(QueryBuilders.termQuery("os", 2))
                .must(QueryBuilders.termQuery("imei", ""))
                .must(QueryBuilders.termQuery("idfa", ""));

        SearchResponse response = null;
        if (client != null) {
            response = client.prepareSearch(index).setTypes(esType).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();

        }
        return response;
    }


    /**
     * 保存设备激活信息
     *
     * @param json
     */
    public IndexResponse saveDeviceInstallForAppID(JSONObject json, int appid) {
        String esType = index + "_APPID_" + appid;
        Client client = ElasticSearchConfig.getClient();
        Map<String, Object> source = esDataReduction(json);
        if (client != null) {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, esType);
            return indexRequestBuilder.setSource(source).execute().actionGet();
        }
        return null;
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
