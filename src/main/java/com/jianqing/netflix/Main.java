package com.jianqing.netflix;

import com.jianqing.scala.netflix.ScalaAnalyticsTask;

import java.util.LinkedList;
import java.util.List;

/**
 *Created by jianqing_sun on 11/2/17.
 *
 */
public class Main {
    public static void main(String[] args) {

        TaskInterface dataIngestTask = new DataIngestTask();
        TaskInterface exportTask = new ExportTask();
        TaskInterface scalaAnalyticsTask = new ScalaAnalyticsTask();
        TaskInterface TaskInterface = new JavaAnalyticsTask();
        TaskInterface reportTask = new ReportTask();

        List<TaskInterface> tasks = new LinkedList<>();
        tasks.add(dataIngestTask);
        tasks.add(exportTask);
        tasks.add(scalaAnalyticsTask);
        tasks.add(TaskInterface);
        tasks.add(reportTask);

        for(TaskInterface task : tasks){
            task.init();
            task.run();
            task.clean();
        }

    }
}
