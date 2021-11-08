package com.example.service.price;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.*;

public class LVPSFunctionalityTest extends LVPSTestBase {
    private final static Logger logger = LogManager.getLogger(LVPSFunctionalityTest.class);
    // Stores preset price data for the basic functionality test
    private final PriceRecord[] priceRecordsForCompleteBatchRun1 = new PriceRecord[3]; // store test data for a complete batch run
    private final PriceRecord[] priceRecordsForCompleteBatchRun2 = new PriceRecord[3]; // store test data for a complete batch run
    private final PriceRecord[] priceRecordsForCancelledBatchRun = new PriceRecord[3]; // store test data for a cancelled batch run
    private final PriceRecord[] priceRecordsForIncompleteBatchRun = new PriceRecord[3]; // store test data for a incomplete batch run

    private static void preparePriceRecordsForInCompleteBatchRuns(String instrumentId, PriceRecord[] priceRecords) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < priceRecords.length; i++) {
            priceRecords[i] = new PriceRecord(instrumentId,
                    LocalDateTime.of(random.nextInt(2000, 2021), random.nextInt(1, 13),
                            random.nextInt(1, 29), 0, 0, 0),
                    getRandomPayload());
        }
    }

    private void preparePriceRecordsForCompleteBatchRuns() {
        priceRecordsForCompleteBatchRun1[0] = new PriceRecord("100",
                LocalDateTime.of(2021, 3, 1,0, 0, 0),
                getRandomPayload());

        priceRecordsForCompleteBatchRun1[1] = new PriceRecord("101",
                LocalDateTime.of(2021, 2, 1,0, 0, 0),
                getRandomPayload());

        priceRecordsForCompleteBatchRun1[2] = new PriceRecord("100",
                LocalDateTime.of(2021, 2, 1,0, 0, 0),
                getRandomPayload());

        priceRecordsForCompleteBatchRun2[0] = new PriceRecord("100",
                LocalDateTime.of(2021, 1, 1,0, 0, 0),
                getRandomPayload());

        priceRecordsForCompleteBatchRun2[1] = new PriceRecord("102",
                LocalDateTime.of(2021, 5, 1,0, 0, 0),
                getRandomPayload());

        priceRecordsForCompleteBatchRun2[2] = new PriceRecord("101",
                LocalDateTime.of(2021, 1, 1, 0, 0, 0),
                getRandomPayload());
    }

    private void startAndCompleteBatchRun(PriceRecord[] priceRecords) {
        String batchRunId = service.start();
        service.upload(batchRunId, priceRecords);
        service.complete(batchRunId);
    }

    private void startAndCancelBatchRun(PriceRecord[] priceRecords) {
        String batchRunId = service.start();
        service.upload(batchRunId, priceRecords);
        service.cancel(batchRunId);
    }

    private void startIncompleteBatchRun(PriceRecord[] priceRecords) {
        String batchRunId = service.start();
        service.upload(batchRunId, priceRecords);
    }

    private void checkPricesForInstrumentId100() {
        PriceRecord priceRecord = service.getLastPrice("100");
        logger.info("100 as of {}: {}", LocalDateTime.now().toLocalDate(), priceRecord);
        // Expect the record with the date: 2021-03-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun1[0]);

        priceRecord = service.getLastPrice("100",
                LocalDateTime.of(2021, 2, 1, 0, 0, 0));
        logger.info("100 as of 2021-02-01: {}", priceRecord);
        // Expect the record with the date: 2021-03-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun1[2]);

        priceRecord = service.getLastPrice("100",
                LocalDateTime.of(2021, 2, 28, 0, 0, 0));
        logger.info("100 as of 2021-02-28: {}", priceRecord);
        // Expect the record with the date: 2021-03-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun1[2]);

        priceRecord = service.getLastPrice("100",
                LocalDateTime.of(2017, 2, 1, 0, 0, 0));
        logger.info("100 as of 2017-02-01: {}", priceRecord);
        // Expect no result
        Assert.assertNull(priceRecord);
    }

    private void checkPricesForInstrumentId101() {
        PriceRecord priceRecord = service.getLastPrice("101");
        logger.info("101 as of {}: {}", LocalDateTime.now().toLocalDate(), priceRecord);
        // Expect the record with the date: 2021-02-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun1[1]);

        priceRecord = service.getLastPrice("101",
                LocalDateTime.of(2021, 1, 30, 0, 0, 0));
        logger.info("101 as of 2021-01-01: {}", priceRecord);
        // Expect the record with the date: 2021-01-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun2[2]);

        priceRecord = service.getLastPrice("101",
                LocalDateTime.of(2021, 2, 1, 0, 0, 0));
        logger.info("101 as of 2021-02-01: {}", priceRecord);
        // Expect the record with the date: 2021-02-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun1[1]);

        priceRecord = service.getLastPrice("101",
                LocalDateTime.of(2017, 2, 1, 0, 0, 0));
        logger.info("101 as of 2017-02-01: {}", priceRecord);
        // Expect no result
        Assert.assertNull(priceRecord);
    }

    private void checkPricesForInstrumentId102() {
        PriceRecord priceRecord = service.getLastPrice("102");
        logger.info("102 as of {}: {}", LocalDateTime.now().toLocalDate(), priceRecord);
        // Expect the record with the date: 2021-05-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun2[1]);

        priceRecord = service.getLastPrice("102",
                LocalDateTime.of(2021, 6, 1, 0, 0, 0));
        logger.info("102 as of 2021-06-01: {}", priceRecord);
        // Expect the record with the date: 2021-06-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun2[1]);

        priceRecord = service.getLastPrice("102",
                LocalDateTime.of(2021, 5, 1, 0, 0, 0));
        logger.info("102 as of 2021-05-01: {}", priceRecord);
        // Expect the record with the date: 2021-05-01T00:00:00
        Assert.assertEquals(priceRecord, priceRecordsForCompleteBatchRun2[1]);

        priceRecord = service.getLastPrice("102",
                LocalDateTime.of(2017, 2, 1, 0, 0, 0));
        logger.info("102 as of 2017-02-01: {}", priceRecord);
        // Expect no result
        Assert.assertNull(priceRecord);
    }

    private void checkPricesForNonExistentInstrumentId(String instrumentId) {
        PriceRecord priceRecord = service.getLastPrice(instrumentId);
        logger.info("{} as of {}: {}", instrumentId, LocalDateTime.now().toLocalDate(), priceRecord);
        // Expect no result
        Assert.assertNull(priceRecord);

        LocalDateTime randomDateTime = getRandomDateTime();
        priceRecord = service.getLastPrice(instrumentId, randomDateTime);
        logger.info("{} as of {}: {}", instrumentId, randomDateTime.toLocalDate(), priceRecord);
        // Expect no result
        Assert.assertNull(priceRecord);
    }


    /**
     * Tests the basic functionality of the service with a limited preset price record content.
     * Two producer threads provide the same prices periodically and four
     * requester threads request prices for different instrument ids periodically.
     * With this test, we ensure that:
     *  - We get correct price records for different instrument ids and dates.
     *  - We get no result (=null) for instrumentIds that doesn't exist.
     *  - We get no result (=null) if the specified date is earlier than the date of the price record that
     *    has the smallest value for the given instrument id.
     *  - Price records with the same instrument id and with the same date given in different batch runs
     *    are not duplicated in the price record map.
     *  - Multiple providers and requesters running in parallel don't create race condition
     *  - Cancelled batch runs are not included in the price map
     *  - Incomplete batch runs are not included in the price map
     *
     * We expect the price tree be built like this in this test:
     * ------------- 100 -----------------
     * [100, 2021-01-01T00:00:00, 0.8733336052982119]
     * [100, 2021-02-01T00:00:00, 0.7845389163701139]
     * [100, 2021-03-01T00:00:00, 0.5261506808228793]
     * --------------101 -----------------
     * [101, 2021-01-01T00:00:00, 0.7660862922208356]
     * [101, 2021-02-01T00:00:00, 0.12916800114813198]
     * --------------102 -----------------
     * [102, 2021-05-01T00:00:00, 0.129749102848997]
     */
    @Test
    public void testFunctionality() {
        service.run();

        // prepare price data for the basic functionality test
        preparePriceRecordsForCompleteBatchRuns(); // prepare priceRecordsForCompleteBatchRun1 and priceRecordsForCompleteBatchRun2
        preparePriceRecordsForInCompleteBatchRuns("103", priceRecordsForCancelledBatchRun);
        preparePriceRecordsForInCompleteBatchRuns("104", priceRecordsForIncompleteBatchRun);

        // ------ schedule providers -------

        scheduleAtFixedRateWithCaution(30, () -> startAndCompleteBatchRun(priceRecordsForCompleteBatchRun1));
        scheduleAtFixedRateWithCaution(40, () -> startAndCompleteBatchRun(priceRecordsForCompleteBatchRun2));
        scheduleAtFixedRateWithCaution(30, () -> startAndCancelBatchRun(priceRecordsForCancelledBatchRun));
        scheduleAtFixedRateWithCaution(40, () -> startIncompleteBatchRun(priceRecordsForIncompleteBatchRun));

        // ------ schedule requesters -------

        try {
            // let producers upload records, or assertions in check methods will fail
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        scheduleAtFixedRateWithCaution(50, this::checkPricesForInstrumentId100);
        scheduleAtFixedRateWithCaution(40, this::checkPricesForInstrumentId101);
        scheduleAtFixedRateWithCaution(50, this::checkPricesForInstrumentId102);
        scheduleAtFixedRateWithCaution(30, () -> checkPricesForNonExistentInstrumentId("103"));
        scheduleAtFixedRateWithCaution(40, () -> checkPricesForNonExistentInstrumentId("104"));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(service.getNumberOfPriceRecords(), 6);

        service.terminate();
        executor.shutdownNow();
    }

    @Test
    public void testBatchRunIdManagement() {
        PriceRecord[] priceRecords = new PriceRecord[1];
        prepareRandomPriceRecords(priceRecords);

        // try to do upload with an invalid batch run id
        Assert.assertFalse(service.upload("NO_SUCH_ID", priceRecords));

        // try to do upload with a null batch run id
        Assert.assertFalse(service.upload(null, priceRecords));

        // try to do complete with an invalid batch run id
        Assert.assertFalse(service.complete("NO_SUCH_ID"));

        // try to do complete with a null batch run id
        Assert.assertFalse(service.complete(null));

        // try to do cancel with an invalid batch run id
        Assert.assertFalse(service.cancel("NO_SUCH_ID"));

        // try to do cancel with a null batch run id
        Assert.assertFalse(service.cancel(null));
    }

    @Test
    public void testBatchRunWithoutUpload() {
        service.run();

        String batchRunId;
        boolean result;

        // complete an empty batch run

        batchRunId= service.start();
        Assert.assertNotNull(batchRunId);

        result = service.complete(batchRunId);
        Assert.assertTrue(result);

        try {
            // sleep some time to be sure to get complete request to be consumed
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(service.getNumberOfPriceRecords(), 0);

        // cancel an empty batch run

        batchRunId= service.start();
        Assert.assertNotNull(batchRunId);

        result = service.cancel(batchRunId);
        Assert.assertTrue(result);

        try {
            // sleep some time to be sure to get cancel request to be consumed
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(service.getNumberOfPriceRecords(), 0);

        service.terminate();
    }

    @Test
    public void testPriceRequestWithNullParams() {
        Assert.assertNull(service.getLastPrice(null));
        Assert.assertNull(service.getLastPrice(null, getRandomDateTime()));
        Assert.assertNull(service.getLastPrice("NO_SUCH_INSTRUMENT", null));
        Assert.assertNull(service.getLastPrice(null, null));
    }
}
