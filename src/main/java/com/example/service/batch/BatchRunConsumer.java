package com.example.service.batch;

import com.example.service.store.RecordStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes the batch run requests in the queue.
 * [DESIGN DECISION]: This class is not thread safe since it's intended to be used by a single queue consumer thread.
 */
public class BatchRunConsumer<T> {
    private final HashMap<String, BatchRun<T>> batchRuns = new HashMap<>();
    private final BlockingQueue<BatchRunRequest<T>> batchRunRequests;
    private final RecordStore<T> recordStore;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final static Logger logger = LogManager.getLogger(BatchRunConsumer.class);


    public BatchRunConsumer(BlockingQueue<BatchRunRequest<T>> batchRunRequests,
                            RecordStore<T> recordStore) {
        this.batchRunRequests = batchRunRequests;
        this.recordStore = recordStore;
    }

    private void createBatchRun(String batchRunId) {
        batchRuns.put(batchRunId, new BatchRun<>(batchRunId));
    }

    private void addBatch(String batchRunId, T[] records) {
        BatchRun<T> batchRun = batchRuns.get(batchRunId);

        if (batchRun == null) {
            logger.warn("Cannot add batch for batch run {}", batchRunId);
            return;
        }

        batchRun.addBatch(records);
    }

    private void completeBatchRun(String batchRunId) {
        BatchRun<T> batchRun = batchRuns.get(batchRunId);

        if (batchRun == null) {
            logger.warn("Cannot complete the batch run {}", batchRunId);
            return;
        }

        batchRuns.remove(batchRunId);
        batchRun.appendTo(recordStore);
        batchRun.clear(); // clear the batches for memory reclaim
    }

    private void cancelBatchRun(String batchRunId) {
        BatchRun<T> batchRun = batchRuns.get(batchRunId);

        if (batchRun == null) {
            logger.warn("Cannot cancel the batch run {}", batchRunId);
            return;
        }

        batchRuns.remove(batchRunId);
        batchRun.clear(); // clear the batches for memory reclaim
    }

    private void consumeRequest() {
        try {
            BatchRunRequest<T> request = batchRunRequests.take();
            logger.info("Consumer received command {} for batch run {}", request.command, request.batchRunId);

            switch (request.command) {
                case CREATE:
                    createBatchRun(request.batchRunId);
                    break;
                case ADD:
                    addBatch(request.batchRunId, request.records);
                    break;
                case COMPLETE:
                    completeBatchRun(request.batchRunId);
                    break;
                case CANCEL:
                    cancelBatchRun(request.batchRunId);
                    break;
                default:
                    logger.warn("Unknown BatchRunRequest command for batch run {}", request.batchRunId);
            }

        } catch (InterruptedException e) {
            stop();
        }
    }

    public void stop() {
        stopped.set(true);
    }

    public void runConsumeRequest() {
        logger.debug("Batch run consumer started");

        while (!stopped.get()) {
            consumeRequest();
        }

        logger.debug("Batch run consumer stopped");
    }
}
