package com.jiuxiu.yxstat.dao.stat.userstatistics;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/13.
 *
 * @author ZhouFy
 */
public class UserRegisterLoginDao extends StatDataBase implements Serializable {

    private static UserRegisterLoginDao userRegisterLoginDao = new UserRegisterLoginDao();

    private UserRegisterLoginDao() {
    }

    public static UserRegisterLoginDao getInstance() {
        return userRegisterLoginDao;
    }

    Object readResolve() {
        return userRegisterLoginDao;
    }


    /**
     * 保存平台用户数
     *
     * @param date                 日期
     * @param loginCount           登录数
     * @param guestRegisterCount   游客注册数
     * @param accountRegisterCount 账号注册数
     * @param phoneRegisterCount   手机号注册数
     * @param qqRegisterCount      qq注册数
     * @param wxRegisterCount      微信注册数
     * @return
     */
    public int savePlatformUserRegisterLoginCount(String date, long loginCount, long guestRegisterCount, long accountRegisterCount, long phoneRegisterCount,
                                                  long qqRegisterCount, long wxRegisterCount, long otherRegisterCount) {
        String querySQL = "select id from stat_user_register_login where date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_user_register_login (login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, other_register_count, date )" +
                    " values (?,?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{loginCount, guestRegisterCount, accountRegisterCount, phoneRegisterCount, qqRegisterCount, wxRegisterCount, otherRegisterCount, date});
        } else {
            String sql = "update stat_user_register_login set login_count=?, guest_register_count=?, account_register_count=?, phone_register_count=?," +
                    " qq_register_count=?, wx_register_count=? , other_register_count = ? where date = ?";
            return executeSql(sql, new Object[]{loginCount, guestRegisterCount, accountRegisterCount, phoneRegisterCount, qqRegisterCount, wxRegisterCount, otherRegisterCount, date});
        }
    }


    /**
     * 保存平台用户数 分钟
     *
     * @param time                 时间
     * @param loginCount           登录数
     * @param guestRegisterCount   游客注册数
     * @param accountRegisterCount 账号注册数
     * @param phoneRegisterCount   手机号注册数
     * @param qqRegisterCount      qq注册数
     * @param wxRegisterCount      微信注册数
     * @return
     */
    public int savePlatformMinuteRegisterLoginCount(String time, long loginCount, long guestRegisterCount, long accountRegisterCount, long phoneRegisterCount,
                                                    long qqRegisterCount, long wxRegisterCount , long otherRegisterCount) {
        String querySQL = "select id from stat_user_register_login_minute where time = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{time});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_user_register_login_minute (login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, other_register_count, time )" +
                    " values (?,?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{loginCount, guestRegisterCount, accountRegisterCount, phoneRegisterCount, qqRegisterCount, wxRegisterCount, otherRegisterCount, time});
        } else {
            String sql = "update stat_user_register_login_minute set login_count=?, guest_register_count=?, account_register_count=?, phone_register_count=?," +
                    " qq_register_count=?, wx_register_count=? , other_register_count = ? where time = ?";
            return executeSql(sql, new Object[]{loginCount, guestRegisterCount, accountRegisterCount, phoneRegisterCount, qqRegisterCount, wxRegisterCount, otherRegisterCount, time});
        }
    }
}
