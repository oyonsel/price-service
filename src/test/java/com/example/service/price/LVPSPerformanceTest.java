package com.example.service.price;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class LVPSPerformanceTest extends LVPSTestBase {
    private final static Logger logger = LogManager.getLogger(LVPSPerformanceTest.class);

    private static ArrayList<PriceRecord[]> prepareRandomPriceRecordsForBatchRun(int chunkFactor, int stopAt) {
        ArrayList<PriceRecord[]> priceRecordsForBatchRun = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numberOfRecords = 0;
        int totalNumberOfRecords = 0;

        for (; totalNumberOfRecords <= stopAt; totalNumberOfRecords += numberOfRecords) {
            logger.info("-------------- {} already and more {} to go -----------", totalNumberOfRecords, numberOfRecords);
            numberOfRecords = random.nextInt(40, 100) * chunkFactor;
            PriceRecord[] priceRecords = new PriceRecord[numberOfRecords];
            prepareRandomPriceRecords(priceRecords);
            priceRecordsForBatchRun.add(priceRecords);
        }

        logger.info("Will add {} batches for {} records", priceRecordsForBatchRun.size(), totalNumberOfRecords);
        return priceRecordsForBatchRun;
    }

    private void runBatch(ArrayList<PriceRecord[]> priceRecordsForBatchRun) {
        logger.info("====================== Starting producer ======================");

        LocalDateTime start = LocalDateTime.now();
        String batchRunId = service.start();

        priceRecordsForBatchRun.forEach((priceRecords) -> {
            logger.info("Adding {} to batch run: {}", priceRecords.length, batchRunId);
            AbstractMap.SimpleImmutableEntry<Boolean, Duration> pair = runAndMeasureDuration(() -> service.upload(batchRunId, priceRecords));
            Assert.assertTrue(pair.getKey());
            logger.info("Added to batch run: {} in {} ns. Size is {}", batchRunId, pair.getValue().toNanos(), service.getNumberOfPriceRecords());
        });
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Completing batch run: {}", batchRunId);
        AbstractMap.SimpleImmutableEntry<Boolean, Duration> pair = runAndMeasureDuration(() -> service.complete(batchRunId));
        Assert.assertTrue(pair.getKey());
        logger.info("Completed batch run: {} in {} ns. Actual map size is {}", batchRunId, pair.getValue().toNanos(),
                service.getNumberOfPriceRecords());
        logger.info("DURATION for batch run {} is {} ms.", batchRunId, Duration.between(start, LocalDateTime.now()).toMillis());
    }

    /**
     * Demonstrates the performance of the service. Calls to service are measured and printed to stdout in nanoseconds.
     * Demonstration starts 4 batch runs, each in separate threads and schedules two periodic tasks to request
     * the latest price. One of the periodic tasks requests the latest price record with a random instrumentId as of
     * current time, other requests the price record as of a random date.
     * <p>
     * Main thread sleeps 30 seconds before main thread terminates the service, to ensure that the service's internal
     * blocking queue consumer thread finishes the batch run processing.
     * <p>
     * Direct Buffer Memory is allocated for the payload part of the price records. The size of the direct memory
     * buffer that JVM uses should be enough for the records to be allocated. For 4 batch runs with around 1M records
     * (and some more) each with an average of 1024K (or some more) payload, maximum Direct Buffer Memory size
     * should be set around 4200M at least before running this test.
     * <p>
     * *** JVM should be given -XX:MaxDirectMemorySize=4200M option before running this test. ***
     * <p>
     * Do `grep STORE <testoutput>` to see how much time "complete()" processing takes
     */
    @Test
    public void demonstratePerformance() {
        final int NUMBER_OF_BATCH_RUNS = 4;
        service.run();

        scheduleAtFixedRateWithCaution(40, () -> {
            String instrumentId = getRandomInstrumentId();
            LocalDateTime now = LocalDateTime.now();
            AbstractMap.SimpleImmutableEntry<PriceRecord, Duration> pair = runAndMeasureDuration(() -> service.getLastPrice(instrumentId));
            logger.info("{} as of {} : {} in {} ns. Size of price records is {}", instrumentId,
                    now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), pair.getKey(), pair.getValue().toNanos(),
                    service.getNumberOfPriceRecords());
        });

        scheduleAtFixedRateWithCaution(50, () -> {
            String instrumentId = getRandomInstrumentId();
            LocalDateTime asOf = getRandomDateTime();
            AbstractMap.SimpleImmutableEntry<PriceRecord, Duration> pair = runAndMeasureDuration(() -> service.getLastPrice(instrumentId, asOf));
            logger.info("{} as of {} : {} in {} ns. Size of price records is {}", instrumentId,
                    asOf.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), pair.getKey(), pair.getValue().toNanos(),
                    service.getNumberOfPriceRecords());
        });

        ArrayList<ArrayList<PriceRecord[]>> multipleBatchRuns = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_BATCH_RUNS; i++) {
            multipleBatchRuns.add(prepareRandomPriceRecordsForBatchRun(1000, 1000000));
        }

        multipleBatchRuns.forEach((batchRunRecords) -> executor.execute(() -> runWithCaution(() -> runBatch(batchRunRecords))));

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        service.terminate();
        executor.shutdownNow();

        logger.info("Price records size at the end: {}", service.getNumberOfPriceRecords());
    }
}
