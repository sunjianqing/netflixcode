package com.jianqing.netflix;

/**
 * Created by jianqingsun on 10/28/17.
 */
public class ReportTask implements TaskInterface {
    @Override
    public void init() {

    }

    @Override
    public int run() {
        System.out.println("Generating report...");
        return 0;
    }

    @Override
    public void clean() {

    }
}
