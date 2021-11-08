package com.example.service.store;

import com.example.service.price.PriceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * [DESIGN DECISION]: This class hides the implementation that is chosen to store the price records.
 * We may decide to persist the records or switch to HashMap and use manual synchronization without
 * informing the users of the class.
 */
public class PriceRecordStore implements RecordStore<PriceRecord> {
    private final static Logger logger = LogManager.getLogger(PriceRecordStore.class);
    // [DESIGN DECISION]: ConcurrentHashMap and ConcurrentHashMap is chosen to prevent race conditions efficiently.
    // We must keep the price records sorted by date so we need ConcurrentSkipListMap.
    private final Map<String, TreeMap<LocalDateTime, PriceRecord>> instrumentPrices = new HashMap<>();
    private final Lock readLock;
    private final Lock writeLock;

    public PriceRecordStore() {
        ReadWriteLock rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
    }

    @Override
    public PriceRecord getLatest(String instrumentId, LocalDateTime asOf) {
        if (instrumentId == null || asOf == null) return null;

        readLock.lock();
        try {
            TreeMap<LocalDateTime, PriceRecord> records = instrumentPrices.get(instrumentId);
            if (records != null) {
                Map.Entry<LocalDateTime, PriceRecord> entry = records.floorEntry(asOf);
                return entry != null ? entry.getValue() : null;
            }
        } finally {
            readLock.unlock();
        }

        return null;
    }

    @Override
    public void store(List<PriceRecord> priceRecords) {
        writeLock.lock();
        try {
            Instant start = Instant.now();
            priceRecords.forEach((priceRecord -> {
                TreeMap<LocalDateTime, PriceRecord> recordTree
                        = instrumentPrices.computeIfAbsent(priceRecord.instrumentId, k -> new TreeMap<>());
                recordTree.put(priceRecord.asOf, priceRecord);
            }));
            Instant end = Instant.now();
            logger.debug("STORE completed in {} ms", Duration.between(start, end).toMillis());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void dump() {
        readLock.lock();
        try {
            instrumentPrices.forEach((instrument, tree) -> {
                System.out.println("----------------- " + instrument + " -----------------");
                tree.values().forEach(System.out::println);
            });
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int size() {
        readLock.lock();
        try {
            return instrumentPrices.values().stream().mapToInt(TreeMap::size).sum();
        } finally {
            readLock.unlock();
        }
    }
}
