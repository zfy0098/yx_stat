package com.jiuxiu.yxstat.es.deviceinstall;

import com.jiuxiu.yxstat.es.ElasticSearchConfig;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by ZhouFy on 2018/6/12.
 *
 * @author ZhouFy
 */
public class DeviceActivationStatisticsESStorage implements Serializable {

    private static String DEVICE_INSTALL_INDEX = PropertyUtils.getValue("es.device.install.index");

    private static String androidClickIndex = PropertyUtils.getValue("es.android.click.index");


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
        String type = DEVICE_INSTALL_INDEX + "_APPID_" + appid;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("imei", imei))
                .must(QueryBuilders.termQuery("child_id", childID));

        return client.prepareSearch(DEVICE_INSTALL_INDEX).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();
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
        String type = DEVICE_INSTALL_INDEX + "_APPID_" + appid;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("idfa", idfa))
                .must(QueryBuilders.termQuery("child_id", childID));
        return client.prepareSearch(DEVICE_INSTALL_INDEX).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();
    }


    /**
     * @param ip          访问ip
     * @param deviceName  设备名称
     * @param deviceOsVer 设备版本号
     * @return
     */
    public SearchResponse iosSpecialSearchForAppID(String ip, String deviceName, String deviceOsVer, int appid, int childID) {
        Client client = ElasticSearchConfig.getClient();
        String esType = DEVICE_INSTALL_INDEX + "_APPID_" + appid;
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
            response = client.prepareSearch(DEVICE_INSTALL_INDEX).setTypes(esType).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();

        }
        return response;
    }


    /**
     * 保存设备激活信息
     *
     * @param json
     */
    public void saveDeviceInstallForAppID(JSONObject json, int appid) {
        String esType = DEVICE_INSTALL_INDEX + "_APPID_" + appid;
        Client client = ElasticSearchConfig.getClient();
        Map<String, Object> source = esDataReduction(json);
        if (client != null) {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(DEVICE_INSTALL_INDEX, esType);
            indexRequestBuilder.setSource(source).execute().actionGet();
        }
    }



    public int getIPActiveCount(String ip, int appid) {
        int count = 0;
        Client client = ElasticSearchConfig.getClient();

        String nowDate = DateUtil.getNowDate(DateUtil.YYYY_MM_DD);

        long time;
        try {
            SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd");
            Date date = format.parse(nowDate);
            time = date.getTime() / 1000;
        } catch (ParseException e) {
            time = 0;
        }

        String type = DEVICE_INSTALL_INDEX + "_APPID_" + appid;
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("ip", ip))
                .must(QueryBuilders.rangeQuery("install_time").gt(time));
        SearchResponse searchResponse = client.prepareSearch(DEVICE_INSTALL_INDEX).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH).setQuery(queryBuilder).execute().actionGet();

        if (searchResponse != null) {
            count = searchResponse.getHits().getHits().length;
        }
        return count;

    }




    public void updateAndroidClickInfo(String type, String esid){

        Client client = ElasticSearchConfig.getClient();

        String esType = androidClickIndex + "_" + type;

        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(androidClickIndex);
            updateRequest.type(esType);
            updateRequest.id(esid);
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("status", 1)
                    .endObject());

            client.update(updateRequest).get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
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
