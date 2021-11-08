package com.example.service.price;

public interface PriceProvider {
    String start();

    boolean upload(String batchRunId, PriceRecord[] priceRecords);

    boolean complete(String batchRunId);

    boolean cancel(String batchRunId);
}
