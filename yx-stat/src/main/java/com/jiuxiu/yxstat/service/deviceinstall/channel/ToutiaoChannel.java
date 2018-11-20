package com.jiuxiu.yxstat.service.deviceinstall.channel;

import com.jiuxiu.yxstat.es.ElasticSearchConfig;
import com.jiuxiu.yxstat.es.deviceinstall.DeviceActivationStatisticsESStorage;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.MD5Utils;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/6.
 *
 * @author Zhoufy
 */
public class ToutiaoChannel implements Serializable {


    private DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();


    private static ToutiaoChannel toutiaoChannel = null;

    private ToutiaoChannel() {
    }

    public static ToutiaoChannel getInstance() {
        if (toutiaoChannel == null) {
            toutiaoChannel = new ToutiaoChannel();
        }
        return toutiaoChannel;
    }

    public void getAdClickInfo(JSONObject json) {

        System.out.println("头条渠道:" + json.toString());

        int appID = json.getInt("appid");
        int childID = json.getInt("child_id");
        int channelID = json.getInt("channel_id");
        int appChannelID = json.getInt("app_channel_id");
        int packageID = json.getInt("package_id");
        String imei = json.getString("imei");
        String ip = json.getString("client_ip");
        String deviceOSVer = json.getString("device_os_ver");
        String deviceName = json.getString("device_name");

        String yesterday;
        String today = DateUtil.getNowDate("yyyyMMdd");
        try {
            yesterday = DateUtil.getDateAgo(today, 1, "yyyMMdd");
        } catch (Exception e) {
            System.out.println("时间转换异常" + json.toString());
            return;
        }

        String type = yesterday;
        SearchResponse response = getAndroidClickInfo(yesterday, appID, childID, channelID, appChannelID, packageID, imei);
        if (response.getHits().getHits().length == 0) {
            type = today;
            response = getAndroidClickInfo(today, appID, childID, channelID, appChannelID, packageID, imei);
        }

        if (response.getHits().getHits().length == 0) {
            type = yesterday;
            response = getAndroidClickInfo(yesterday, appID, childID, channelID, appChannelID, packageID, ip, deviceName, deviceOSVer);
            if (response.getHits().getHits().length == 0) {
                type = today;
                response = getAndroidClickInfo(today, appID, childID, channelID, appChannelID, packageID, ip, deviceName, deviceOSVer);
            }
        } else {
            System.out.println("通过imei查询广告es成功" + imei);
        }

        if (response.getHits().getHits().length > 0) {

            System.out.println("获取广告点击成功 开始更新es");

            SearchHits searchHits = response.getHits();
            SearchHit[] searchHit = searchHits.getHits();
            SearchHit hit = searchHit[0];

            Map<String, Object> map = hit.getSource();

            String esid = hit.getId();

            // 更新es值
            deviceActivationStatisticsESStorage.updateAndroidClickInfo(type, esid);

            System.out.println("将上报信息保存到redis中");

            JSONObject clickInfo = new JSONObject();
            clickInfo.putAll(json);
            clickInfo.put("ad_click", JSONObject.fromObject(map));
            JedisUtils.listAdd(JedisPoolConfigInfo.adClickPoolKey, "click_device", clickInfo.toString());
        } else {
            System.out.println("查询es广告失败" + json.toString());
        }
    }

    private static String androidClickIndex = PropertyUtils.getValue("es.android.click.index");

    private SearchResponse getAndroidClickInfo(String date, int appid, int childID, int channelID, int appChannelID, int packageID, String imei) {
        Client client = ElasticSearchConfig.getClient();

        String esType = androidClickIndex + "_" + date;

        String md5IMEI = MD5Utils.getMD5(imei);

        System.out.println("md5imei：" + md5IMEI) ;

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("appid" , appid))
                .must(QueryBuilders.termQuery("child_id", childID))
                .must(QueryBuilders.termQuery("channel_id", channelID))
                .must(QueryBuilders.termQuery("app_channel_id" , appChannelID))
                .must(QueryBuilders.termQuery("package_id", packageID))
                .must(QueryBuilders.termQuery("status" , 0))
                .must(QueryBuilders.termQuery("imei", md5IMEI));


        return client.prepareSearch(androidClickIndex).setTypes(esType)
                .setQuery(queryBuilder).addSort("ts", SortOrder.ASC).execute().actionGet();
    }

    private SearchResponse getAndroidClickInfo(String date, int appid, int childID, int channelID, int appChannelID, int packageID, String ip, String deviceName, String osVer) {
        Client client = ElasticSearchConfig.getClient();

        String esType = androidClickIndex + "_" + date;

        // .must(QueryBuilders.termQuery("ip", ip))
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("appid", appid))
                .must(QueryBuilders.termQuery("child_id", childID))
                .must(QueryBuilders.termQuery("channel_id", channelID))
                .must(QueryBuilders.termQuery("app_channel_id" , appChannelID))
                .must(QueryBuilders.termQuery("package_id", packageID))
                .must(QueryBuilders.termQuery("status" , 0))
                .must(QueryBuilders.matchQuery("user_agent", deviceName))
                .must(QueryBuilders.matchQuery("user_agent", osVer));

        return client.prepareSearch(androidClickIndex).setTypes(esType)
                .setQuery(queryBuilder).addSort("ts", SortOrder.ASC).execute().actionGet();
    }

}
