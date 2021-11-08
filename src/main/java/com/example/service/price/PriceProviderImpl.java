package com.example.service.price;

import com.example.service.batch.BatchRunRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fills the blocking queue with batch run requests received by API calls from clients.
 */
public class PriceProviderImpl implements PriceProvider {
    private final static Logger logger = LogManager.getLogger(PriceProviderImpl.class);
    private final BlockingQueue<BatchRunRequest<PriceRecord>> batchRunRequests;
    // [DESIGN DECISION]: Store batch run ids in a concurrent map to prevent uploads with wrong ids. The value
    // type of the map represents that any upload is made or not for the key batch run id.
    // When a new batch run id is created it is set to false, and on the first upload, it is flagged as true.
    // By this way, we might set a timer for cleaning up the batch runs with no uploads or the ones that are not
    // completed after a certain time.
    private final ConcurrentHashMap<String, Boolean> batchRunIds = new ConcurrentHashMap<>();

    public PriceProviderImpl(BlockingQueue<BatchRunRequest<PriceRecord>> batchRunRequests) {
        this.batchRunRequests = batchRunRequests;
    }

    private boolean queueRequest(BatchRunRequest<PriceRecord> request) {
        try {
            batchRunRequests.put(request);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public String start() {
        // [DESIGN DECISION]: Use UUID for batch run ids so that there's no need to track available ids.
        String batchRunId = UUID.randomUUID().toString();
        if(!queueRequest(BatchRunRequest.newCreateBatchRunRequest(batchRunId))) return null;

        batchRunIds.put(batchRunId, false);
        return batchRunId;
    }

    @Override
    public boolean upload(String batchRunId, PriceRecord[] priceRecords) {
        if (batchRunId == null) return false;

        Boolean uploaded = batchRunIds.get(batchRunId);
        if (uploaded == null) {
            logger.error("No such active batch run id found: {}", batchRunId);
            return false;
        }

        if (!queueRequest(BatchRunRequest.newAddBatchRunRequest(batchRunId, priceRecords))) return false;

        if (!uploaded) {
            batchRunIds.put(batchRunId, true);
        }

        return true;
    }

    @Override
    public boolean complete(String batchRunId) {
        if (batchRunId == null) return false;

        Boolean uploaded = batchRunIds.get(batchRunId);
        if (uploaded == null) {
            logger.error("No such active batch run id found: {}", batchRunId);
            return false;
        }

        if(!queueRequest(BatchRunRequest.newCompleteBatchRunRequest(batchRunId))) return false;

        if (!uploaded) {
            logger.warn("Completing batch run without any upload: {}", batchRunId);
        }

        batchRunIds.remove(batchRunId);
        return true;
    }

    @Override
    public boolean cancel(String batchRunId) {
        if (batchRunId == null) return false;

        Boolean uploaded = batchRunIds.get(batchRunId);
        if (uploaded == null) {
            logger.error("No such active batch run id found: {}", batchRunId);
            return false;
        }

        if(!queueRequest(BatchRunRequest.newCancelBatchRunRequest(batchRunId))) return false;

        batchRunIds.remove(batchRunId);
        return true;
    }
}
