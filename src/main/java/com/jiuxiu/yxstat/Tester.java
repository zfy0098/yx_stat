package com.jiuxiu.yxstat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jiuxiu.yxstat.utils.ConstantTest;
import org.elasticsearch.action.support.ThreadedActionListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class Tester {


    public static void main(String[] args) throws  Exception{

        StringBuffer key = new StringBuffer("123");

        System.out.println(key.toString());

    }
}
