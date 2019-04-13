package com.raft.demo;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class ThreadPoolDemo {
    public static void main(String[] args) {
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(3);

        threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                System.out.println("1");
            }
        }, 3000, 2000, TimeUnit.MILLISECONDS);

        threadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("2");
            }
        }, 3000, 2000,TimeUnit.MILLISECONDS );


    }
}
