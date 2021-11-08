package com.example.service.batch;

/**
 * This class represents batch run requests which can have different types, each
 * representing a new batch run command. The instances of this class are
 * aimed to be queued to a queue by a producer and de-queued by a consumer.
 * @param <T> type of records that this batch run request points to
 */
public class BatchRunRequest<T> {
    final BatchRunCommand command;
    final String batchRunId;
    final T[] records;

    enum BatchRunCommand {
        CREATE, ADD, COMPLETE, CANCEL
    }

    private BatchRunRequest(BatchRunCommand command, String batchRunId, T[] records) {
        this.command = command;
        this.batchRunId = batchRunId;
        this.records = records;
    }

    public static <T> BatchRunRequest<T> newCreateBatchRunRequest(String batchRunId) {
        return new BatchRunRequest<>(BatchRunCommand.CREATE, batchRunId, null);
    }

    public static <T> BatchRunRequest<T> newAddBatchRunRequest(String batchRunId, T[] records) {
        return new BatchRunRequest<>(BatchRunCommand.ADD, batchRunId, records);
    }

    public static <T> BatchRunRequest<T> newCompleteBatchRunRequest(String batchRunId) {
        return new BatchRunRequest<>(BatchRunCommand.COMPLETE, batchRunId, null);
    }

    public static <T> BatchRunRequest<T> newCancelBatchRunRequest(String batchRunId) {
        return new BatchRunRequest<>(BatchRunCommand.CANCEL, batchRunId, null);
    }
}
