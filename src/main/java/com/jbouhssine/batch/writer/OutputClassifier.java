package com.jbouhssine.batch.writer;

import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

public class OutputClassifier implements Classifier<String, ItemWriter<? super String>> {

    private final ItemWriter<String> okWriter;
    private final ItemWriter<String> t01Writer;
    private final ItemWriter<String> t19Writer;
    private final ItemWriter<String> topoWriter;

    public OutputClassifier(ItemWriter<String> okWriter,
                            ItemWriter<String> t01Writer,
                            ItemWriter<String> t19Writer,
                            ItemWriter<String> topoWriter) {
        this.okWriter = okWriter;
        this.t01Writer = t01Writer;
        this.t19Writer = t19Writer;
        this.topoWriter = topoWriter;
    }

    @Override
    public ItemWriter<? super String> classify(String item) {
        System.out.println("Using writer: " + item);
        if (item.contains("STATUS=T01")) return t01Writer;
        if (item.contains("STATUS=T19")) return t19Writer;
        if (item.contains("STATUS=TOPO")) return topoWriter;
        return okWriter;
    }
}
