package com.jiuxiu.yxstat;

import com.jiuxiu.yxstat.enums.RegisterType;
import com.jiuxiu.yxstat.utils.DateUtil;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class Tester {
    public static void main(String[] args){

        System.out.println(DateUtil.getNowFutureWhileMinute(1530079947L));
        String time = "2018-06-27 14:20:00";

        String nowTime = time.substring(0 , 4).replace("-" , "");
        System.out.println(nowTime);
    }
}
