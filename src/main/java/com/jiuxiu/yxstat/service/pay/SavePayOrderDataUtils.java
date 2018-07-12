package com.jiuxiu.yxstat.service.pay;

import com.jiuxiu.yxstat.dao.stat.payorder.StatPayOrderDao;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.payorder.JedisPayOrderKeyConstant;

/**
 * Created with IDEA by ZhouFy on 2018/7/9.
 *
 * @author ZhouFy
 */
public class SavePayOrderDataUtils {

    private static StatPayOrderDao statPayOrderDao = StatPayOrderDao.getInstance();

    public static void savePayOrderData(String date, int appID , int childID, int channelID , int appChannelID , int packageID, int payOrderCount, int payTotalAmount){
        long appIDPayUserCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.APP_ID_PAY_ORDER + appID);
        statPayOrderDao.saveAppIDPayOrder(date, appID, appIDPayUserCount, payTotalAmount ,payOrderCount);

        long childIDPayUserCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.CHILD_ID_PAY_ORDER + childID + ":" + appID);
        statPayOrderDao.saveChildIDPayOrder(date , appID, childID, childIDPayUserCount, payTotalAmount ,payOrderCount  );

        long channelIDPayUserCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.CHANNEL_ID + channelID + ":" + childID + ":" + appID);
        statPayOrderDao.saveChannelIDPayOrder(date, appID,childID, channelID, channelIDPayUserCount, payTotalAmount, payOrderCount);

        long appChannelPayUserCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.APP_CHANNEL_ID + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        statPayOrderDao.saveAppChannelIDPayOrder(date, appID,childID, channelID, appChannelID , appChannelPayUserCount, payTotalAmount, payOrderCount);

        long packagePayUserCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.PACKAGE_ID + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        statPayOrderDao.savePackageIDPayOrder(date, appID,childID, channelID, appChannelID , packageID,  packagePayUserCount, payTotalAmount, payOrderCount);
    }

    public static void savePlatformPayOrderData(String date , int payOrderCount, int payTotalAmount){
        long payUserCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.PLATFORM_PAY_ORDER );
        statPayOrderDao.savePlatformPayOrder(date,payUserCount,payTotalAmount,payOrderCount);
    }
}
