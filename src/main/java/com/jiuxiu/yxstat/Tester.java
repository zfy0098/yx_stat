package com.jiuxiu.yxstat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jiuxiu.yxstat.dao.ConfigDao;
import com.jiuxiu.yxstat.dao.PayDao;
import com.jiuxiu.yxstat.dao.UserDao;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class Tester {


    private static Logger log = LoggerFactory.getLogger(Tester.class);

    public static void main(String[] args) {
        int x = 1;

        while (true) {
            x++;
            int threadCount = 10;
            ThreadFactory threadFactory = new ThreadFactoryBuilder().build();
            ExecutorService executor = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingDeque<>(), threadFactory, new ThreadPoolExecutor.AbortPolicy());

            for (int i = 0; i < 10; i++) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        PayDao payDao = PayDao.getPayDao();
                        payDao.init();
                    }
                });
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        UserDao userDao = UserDao.getUserDao();
                        userDao.init();
                    }
                });

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        ConfigDao configDao = ConfigDao.getConfigDao();
                        configDao.init();
                    }
                });
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        String name = JedisUtils.get(JedisPoolConfigInfo.configRedisPoolKey, "name");
                        log.info(name);
                    }
                });

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        String name = JedisUtils.get(JedisPoolConfigInfo.payRedisPoolKey, "test");
                        log.info(name);
                    }
                });

            }
            executor.shutdown();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (x == 15) {
                log.info("程序退出");
                System.exit(1);
            }
        }
    }
}
