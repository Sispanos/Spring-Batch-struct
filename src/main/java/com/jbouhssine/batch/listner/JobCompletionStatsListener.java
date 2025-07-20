package com.jbouhssine.batch.listner;


import org.springframework.batch.core.*;
import org.springframework.stereotype.Component;

import java.io.*;

@Component
public class JobCompletionStatsListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        try {
            System.out.println("✔ Batch terminé.");
            System.out.println("→ Lignes lues : " +
                    jobExecution.getStepExecutions().stream().mapToInt(StepExecution::getReadCount).sum());

            for (String file : new String[]{
                    "resultatok.csv", "anomalieT01.csv", "anomalieT19.csv", "anomalieCodeTopo.csv"
            }) {
                long count = new BufferedReader(new FileReader(file)).lines().count() - 1; // ignore header
                System.out.println("→ " + file + ": " + count + " lignes");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}