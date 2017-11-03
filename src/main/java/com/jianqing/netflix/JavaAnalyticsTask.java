package com.jianqing.netflix;

/**
 * Created by jianqingsun on 10/31/17.
 */
class JavaAnalyticsTask implements TaskInterface {

    @Override
    public void init() {

    }

    @Override
    public int run() {
        System.out.println("Running Java analytics task...");
        return 0;
    }

    @Override
    public void clean() {
        System.out.println("Cleaning Java analytics task...");
    }
}
