package com.jiuxiu;


import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

/**
 * Created with IDEA by ZhouFy on 2018/6/28.
 *
 * @author ZhouFy
 */
public class TestCase {


    private Logger log = Logger.getLogger("user_login");

    private Logger userRegister = Logger.getLogger("user_register");


    private Logger error = Logger.getLogger("error");

    public void test(){
        log.info("user_login");
        userRegister.info("user_register");
        error.error("error");
    }

    public static void main(String[] args){
        TestCase t = new TestCase();
        t.test();
    }
}
