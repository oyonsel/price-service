package com.example.service.price;

import com.example.service.store.PriceRecordStore;

import java.time.LocalDateTime;

public class PriceRequesterImpl implements PriceRequester {
    private final PriceRecordStore priceRecordStore;

    public PriceRequesterImpl(PriceRecordStore priceRecordStore) {
        this.priceRecordStore = priceRecordStore;
    }

    @Override
    public PriceRecord getLastPrice(String instrumentId) {
        return getLastPrice(instrumentId, LocalDateTime.now());
    }

    @Override
    public PriceRecord getLastPrice(String instrumentId, LocalDateTime asOf) {
        return priceRecordStore.getLatest(instrumentId, asOf);
    }
}
