package com.nextjoy.retention.utils;

import com.nextjoy.retention.constant.Constant;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created with IDEA by hadoop on 2018/8/17.
 *
 * @author Choufy
 */
public class DateUtils {

    public final static String YYYY_MM_DD = "yyyy-MM-dd";


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


    public static String getData(Date date,  String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    public static String dateAgo(String nowDate, int day) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        Date date = sdf.parse(nowDate);
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -day);
        date = calendar.getTime();
        return sdf.format(date);
    }



    /**
     * 获取日期list
     *
     * @return
     */
    public static List<String> dateAgo(String nowDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        List<String> list = new ArrayList<>(20);
        Date date;
        for (int day : Constant.days) {
            try {
                date = sdf.parse(nowDate);
                calendar.setTime(date);
                calendar.add(Calendar.DAY_OF_MONTH, -day);
                date = calendar.getTime();
                list.add(sdf.format(date));
            } catch (ParseException e) {
                System.exit(1);
            }
        }
        return list;
    }

    public static List<String> getBetweenDates(String startTime, String endTime, String format) throws Exception{

        List<String> dataList = new ArrayList<>();
        dataList.add(startTime);

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date dBegin = sdf.parse(startTime);
        Date dEnd = sdf.parse(endTime);

        Calendar calBegin = Calendar.getInstance();
        // 使用给定的 Date 设置此 Calendar 的时间
        calBegin.setTime(dBegin);
        Calendar calEnd = Calendar.getInstance();
        // 使用给定的 Date 设置此 Calendar 的时间
        calEnd.setTime(dEnd);
        // 测试此日期是否在指定日期之后
        while (dEnd.after(calBegin.getTime())) {
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
            calBegin.add(Calendar.DAY_OF_MONTH, 1);
            dataList.add(sdf.format(calBegin.getTime()));
        }
        return dataList;
    }


    /**|
     *   计算两个日期相差几天
     * @param staDay
     * @param endDay
     * @return
     * @throws ParseException
     */
    public static long differentDays(String staDay, String endDay) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date1 = sdf.parse(staDay);
        Date date2 = sdf.parse(endDay);

        Calendar cal = Calendar.getInstance();
        cal.setTime(date1);
        long time1 = cal.getTimeInMillis();
        cal.setTime(date2);
        long time2 = cal.getTimeInMillis();
        return (time2 - time1) / (1000 * 3600 * 24);
    }


    public static void main(String[] args) throws Exception{
        long days = DateUtils.differentDays("2018-10-31", "2018-11-13");
        System.out.println(days);
    }




}
