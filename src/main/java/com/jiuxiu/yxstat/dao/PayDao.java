package com.jiuxiu.yxstat.dao;

import com.jiuxiu.yxstat.db.PayDataBase;

/**
 * Created with IDEA by Zhoufy on 2018/5/11.
 *
 * @author Zhoufy
 */
public class PayDao extends PayDataBase {


    private static PayDao payDao = null;


    private PayDao(){}

    public static PayDao getInstance(){
        if(payDao == null){
            payDao = new PayDao();
        }
        return payDao;
    }



    public void init(){
        String sql = "select * from pay_order limit 10";

        log.info(queryForList(sql , null).toString());

    }

}
