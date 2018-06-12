package com.jiuxiu.yxstat.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtil {


    public final static String yyyy_MM_dd = "yyyy-MM-dd";



    public static String getNowDate(String format){
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(calendar.getTime());
    }


}
