package com.example.service.price;

import org.junit.After;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class LVPSTestBase {
    LastValuePriceService service;
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    // [DESIGN DECISION]: In order to detect and store failures in scheduled threads in the tests,
    // an AtomicReference is used that stores the Throwable object that might be caught in one of the
    // threads in the tests, either because an assertion is failed or some unexpected failure.
    // At the end of each test the reference is checked if it references a Throwable instance.
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    static <T> AbstractMap.SimpleImmutableEntry<T, Duration> runAndMeasureDuration(Supplier<T> method) {
        Instant start = Instant.now();
        T result = method.get();
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        return new AbstractMap.SimpleImmutableEntry<>(result, duration);
    }

    static String getRandomInstrumentId() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return String.valueOf(random.nextInt(0, 1000));
    }

    static ByteBuffer getRandomPayload() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // [DESIGN DECISION]: We use Direct Buffer Memory allocation for payload. By this way,
        // we do not use heap memory which is garbage collected and has a low I/O performance.
        return ByteBuffer.allocateDirect(random.nextInt(1016, 1033))
                .putDouble(random.nextDouble());
    }

    static LocalDateTime getRandomDateTime() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return LocalDateTime.of(random.nextInt(2000, 2018), random.nextInt(1, 13),
                random.nextInt(1, 29), random.nextInt(0, 24),
                random.nextInt(0, 60), random.nextInt(0, 60));
    }

    static PriceRecord getRandomPriceRecord() {
        return new PriceRecord(getRandomInstrumentId(),
                getRandomDateTime(),
                getRandomPayload());
    }

    static void prepareRandomPriceRecords(PriceRecord[] priceRecords) {
        for (int i = 0; i < priceRecords.length; i++) {
            priceRecords[i] = getRandomPriceRecord();
        }
    }

    void runWithCaution(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            failure.set(e);
        }
    }

    ScheduledFuture<?> scheduleAtFixedRateWithCaution(long period, Runnable command) {
        return executor.scheduleAtFixedRate(() -> runWithCaution(command), 0, period, TimeUnit.MILLISECONDS);
    }

    void resetFailure() {
        failure.set(null);
    }

    void checkFailure() {
        Throwable result = failure.get();
        if (result != null) {
            // Fail the test with an AssertionError that contains the cause
            throw new AssertionError(result);
        }
    }

    @Before
    public void prepareTest() {
        // start each test with a fresh instance
        service = new LastValuePriceService();
        resetFailure();
    }

    @After
    public void endTest() {
        service = null; // make service available for garbage collection
        checkFailure();
    }
}
