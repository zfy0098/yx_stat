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

}
