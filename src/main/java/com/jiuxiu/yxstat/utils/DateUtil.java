package com.jiuxiu.yxstat.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hadoop
 */
public class DateUtil {


    public final static String YYYY_MM_DD = "yyyy-MM-dd";

    /**
     * 时间阈值
     */
    private static final int TIME_THRESHOLD = 10;

    /**
     * 最大分钟数
     */
    private static final int MAX_MINUTE = 59;

    private static SimpleDateFormat sdf_yyyy_mm_dd = new SimpleDateFormat(YYYY_MM_DD);

    private static Map<String, SimpleDateFormat> map = new HashMap<>();

    static {
        map.put(YYYY_MM_DD, sdf_yyyy_mm_dd);
    }


    /**
     * 获取当前时间
     *
     * @param format 时间格式
     * @return
     */
    public static String getNowDate(String format) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(calendar.getTime());
    }

    /**
     * 获取过去最近时间的分钟整数
     *
     * @return
     */
    public static String getNowPastWholeMinute() {
        Calendar calendar = Calendar.getInstance();
        return getTimeStr(calendar);
    }


    /**
     * 获取未来最近时间的分钟整数
     *
     * @return
     */
    public static String getNowFutureWhileMinute() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(calendar.getTimeInMillis() + TIME_THRESHOLD * 60 * 1000);
        return getTimeStr(calendar);
    }


    /**
     * 获取执行时间（秒）未来最近的时间分钟整数
     *
     * @param second
     * @return
     */
    public static String getNowFutureWhileMinute(long second) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(second * 1000 + TIME_THRESHOLD * 60 * 1000);
        return getTimeStr(calendar);
    }


    private static String getTimeStr(Calendar calendar) {
        List<Integer> minuteList = new ArrayList<>();
        int minuteCertificate = 0;
        for (int i = 0; minuteCertificate < MAX_MINUTE; ) {
            minuteCertificate = ++i * TIME_THRESHOLD;
            minuteList.add(minuteCertificate);
        }
        StringBuilder time = new StringBuilder();
        time.append(map.get(YYYY_MM_DD).format(calendar.getTimeInMillis()));
        time.append(" ");
        time.append(calendar.get(Calendar.HOUR_OF_DAY));
        time.append(":");
        int minute = calendar.get(Calendar.MINUTE);
        for (Integer m : minuteList) {
            if (minute < m) {
                minute = m - TIME_THRESHOLD;
                if (minute < 10) {
                    time.append("0");
                }
                time.append(minute);
                break;
            }
        }
        time.append(":00");
        return time.toString();
    }

    /**
     * 将毫秒转成时间
     *
     * @param second 时间戳(秒)
     * @param format 转化的格式
     * @return 时间字符串
     */
    public static String secondToDateString(long second, String format) {
        return map.get(format).format(new Date(second * 1000));
    }
}
