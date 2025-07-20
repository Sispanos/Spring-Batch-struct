package com.jbouhssine.batch.processor;



import org.springframework.batch.item.ItemProcessor;

public class LineItemProcessor implements ItemProcessor<String, String> {

    @Override
    public String process(String line) {
        String[] parts = line.split(",");
        String code = parts.length > 1 ? parts[1] : "";
        String description = parts.length > 2 ? parts[2] : "";

        boolean ws1 = code.startsWith("A");
        boolean ws2 = description.contains("Topo");

        String status;
        if (!ws1) status = "T01";
        else if (!ws2) status = "T19";
        else if (code.equals("999")) status = "TOPO";
        else status = "OK";

        return line + ",STATUS=" + status;
    }
}