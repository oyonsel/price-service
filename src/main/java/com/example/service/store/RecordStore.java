package com.example.service.store;

import java.time.LocalDateTime;
import java.util.List;

/**
 * This interface represents a record storage that clients can use to store records
 * and request the latest record based on the given date and instrument id.
 * @param <T> type of records to be stored
 */
public interface RecordStore<T> {
    T getLatest(String instrumentId, LocalDateTime asOf);

    void store(List<T> records);

    void dump();

    int size();
}
