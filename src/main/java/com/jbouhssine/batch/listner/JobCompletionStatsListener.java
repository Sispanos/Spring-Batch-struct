package com.jbouhssine.batch.listner;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@Component
public class JobCompletionStatsListener implements JobExecutionListener {

    @Value("${output.ok.file}")
    private String okFile;

    @Value("${output.t01.file}")
    private String t01File;

    @Value("${output.t19.file}")
    private String t19File;

    @Value("${output.topo.file}")
    private String topoFile;

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        try {
            System.out.println("\n✔ Batch terminé.");

            int totalRead = jobExecution.getStepExecutions().stream()
                    .mapToInt(StepExecution::getReadCount)
                    .sum();

            System.out.println("→ Lignes lues (input) : " + totalRead);
            printFileCount(okFile, "✔ resultatok.csv");
            printFileCount(t01File, "✔ anomalieT01.csv");
            printFileCount(t19File, "✔ anomalieT19.csv");
            printFileCount(topoFile, "✔ anomalieCodeTopo.csv");

        } catch (Exception e) {
            System.err.println("❌ Erreur lors du calcul des statistiques : " + e.getMessage());
        }
    }

    private void printFileCount(String path, String label) {
        File file = new File(path);
        if (!file.exists()) {
            System.out.println(label + ": Fichier introuvable.");
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            long lineCount = reader.lines().count();
            System.out.println(label + ": " + Math.max(0, lineCount - 1) + " lignes (hors en-tête)");
        } catch (IOException e) {
            System.out.println(label + ": Erreur de lecture → " + e.getMessage());
        }
    }
}
