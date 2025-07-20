package com.jbouhssine.batch.config;

import com.jbouhssine.batch.listner.JobCompletionStatsListener;
import com.jbouhssine.batch.writer.OutputClassifier;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final String OUTPUT_DIR = "src/main/resources/output/";

    private final StepBuilderFactory stepBuilderFactory;
    private final PlatformTransactionManager transactionManager;

    public BatchConfig(StepBuilderFactory stepBuilderFactory, PlatformTransactionManager transactionManager) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.transactionManager = transactionManager;
    }

    @Bean
    public FlatFileItemReader<String> reader() {
        return new FlatFileItemReaderBuilder<String>()
                .name("stringReader")
                .resource(new ClassPathResource("input.csv"))
                .linesToSkip(1)
                .strict(true)
                .lineMapper((line, lineNumber) -> line)
                .build();
    }

    @Bean
    public ItemProcessor<String, String> processor() {
        return line -> {
            if (line.contains("T01")) return line + ",STATUS=T01";
            if (line.contains("T19")) return line + ",STATUS=T19";
            if (line.contains("TOPO")) return line + ",STATUS=TOPO";
            return line + ",STATUS=OK";
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<String> okWriter() {
        return buildWriter(OUTPUT_DIR + "resultatok.csv", "id,code,description");
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<String> t01Writer() {
        return buildWriter(OUTPUT_DIR + "anomalieT01.csv", "id,code,description");
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<String> t19Writer() {
        return buildWriter(OUTPUT_DIR + "anomalieT19.csv", "id,code,description");
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<String> topoWriter() {
        return buildWriter(OUTPUT_DIR + "anomalieCodeTopo.csv", "id,code,description");
    }

    private FlatFileItemWriter<String> buildWriter(String path, String header) {
        ensureFileWritable(path);
        return new FlatFileItemWriterBuilder<String>()
                .name("writer-" + new File(path).getName())
                .resource(new FileSystemResource(path))
                .headerCallback(writer -> writer.write(header))
                .lineAggregator(item -> item.replaceAll(",STATUS=.*$", ""))
                .shouldDeleteIfExists(true)
                .append(false)
                .build();
    }

    private void ensureFileWritable(String path) {
        try {
            File file = new File(path);
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new RuntimeException("Erreur création fichier : " + path, e);
        }
    }

    @Bean
    public ClassifierCompositeItemWriter<String> classifierWriter(
            FlatFileItemWriter<String> okWriter,
            FlatFileItemWriter<String> t01Writer,
            FlatFileItemWriter<String> t19Writer,
            FlatFileItemWriter<String> topoWriter
    ) {
        ClassifierCompositeItemWriter<String> writer = new ClassifierCompositeItemWriter<>();
        writer.setClassifier(new OutputClassifier(okWriter, t01Writer, t19Writer, topoWriter));
        return writer;
    }

    @Bean
    public Step processingStep(
            StepBuilderFactory stepBuilderFactory,
            FlatFileItemReader<String> reader,
            ItemProcessor<String, String> processor,
            ClassifierCompositeItemWriter<String> classifierWriter,
            FlatFileItemWriter<String> okWriter,
            FlatFileItemWriter<String> t01Writer,
            FlatFileItemWriter<String> t19Writer,
            FlatFileItemWriter<String> topoWriter
    ) {
        return stepBuilderFactory.get("processingStep")
                .<String, String>chunk(1000)
                .reader(reader)
                .processor(processor)
                .writer(classifierWriter)
                .stream(okWriter)
                .stream(t01Writer)
                .stream(t19Writer)
                .stream(topoWriter)
                .build();
    }

    @Bean
    public Job myJob(JobRepository jobRepository, Step processingStep) {
        return new JobBuilder("myJob")
                .repository(jobRepository)
                .start(processingStep)
                .listener(new JobCompletionStatsListener())
                .build();
    }
}
