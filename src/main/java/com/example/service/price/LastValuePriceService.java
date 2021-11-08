package com.example.service.price;

import com.example.service.batch.BatchRunConsumer;
import com.example.service.batch.BatchRunRequest;
import com.example.service.store.PriceRecordStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class LastValuePriceService implements PriceProvider, PriceRequester {
    private final static Logger logger = LogManager.getLogger(LastValuePriceService.class);
    private final PriceRecordStore priceRecordStore = new PriceRecordStore();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    // [DESIGN DECISION]: This flag will prevent the service to double start and terminate without a start
    private final AtomicBoolean started = new AtomicBoolean();
    // [DESIGN DECISION]: LastValuePriceService delegates service API calls to the real implementors of the APIs.
    private final PriceRequester priceRequester;
    private final PriceProvider priceProvider;
    private final BatchRunConsumer<PriceRecord> batchRunConsumer;

    public LastValuePriceService() {
        // [DESIGN DECISION]: Using blocking queue to enqueue incoming requests to provide records. By this way,
        // API calls have faster response times. The requests in the queue are consumed by a single consumer thread
        // so that we do not need to worry about synchronization of the batch runs stored. ArrayBlockingQueue is
        // chosen because it performs better than LinkedBlockingQueue but with a predefined capacity specified.
        ArrayBlockingQueue<BatchRunRequest<PriceRecord>> batchRunRequests = new ArrayBlockingQueue<>(1000);
        // [DESIGN DECISION]: The default implementation of the service instantiates the default API implementors
        // to delegate the calls. LastValuePriceService might have another constructor that takes specific
        // PriceProvider and PriceRequester implementations.
        priceRequester = new PriceRequesterImpl(priceRecordStore);
        priceProvider = new PriceProviderImpl(batchRunRequests);
        batchRunConsumer = new BatchRunConsumer<>(batchRunRequests, priceRecordStore);
    }

    public void dumpPrices() {
        priceRecordStore.dump();
    }

    public int getNumberOfPriceRecords() {
        return priceRecordStore.size();
    }

    public void terminate() {
        if (started.get()) {
            logger.debug("Service shutting down");
            executor.shutdownNow();
            try {
                executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        started.set(false);
    }

    public void run() {
        if (!started.getAndSet(true)) {
            executor.execute(batchRunConsumer::runConsumeRequest);
            logger.debug("Service running");
        }
    }

    // ---------- Provider API ----------

    @Override
    public String start() {
        return priceProvider.start();
    }

    @Override
    public boolean upload(String batchRunId, PriceRecord[] priceRecords) {
        return priceProvider.upload(batchRunId, priceRecords);
    }

    @Override
    public boolean complete(String batchRunId) {
        return priceProvider.complete(batchRunId);
    }

    @Override
    public boolean cancel(String batchRunId) {
        return priceProvider.cancel(batchRunId);
    }

    // ---------- Requester API ----------

    @Override
    public PriceRecord getLastPrice(String instrumentId) {
        return priceRequester.getLastPrice(instrumentId);
    }

    @Override
    public PriceRecord getLastPrice(String instrumentId, LocalDateTime asOf) {
        return priceRequester.getLastPrice(instrumentId, asOf);
    }
}
