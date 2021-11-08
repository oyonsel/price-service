package com.example.service.batch;

import com.example.service.store.RecordStore;

import java.util.*;

/**
 * This class represents a batch run and retains a list of records uploaded in batches. Each uploaded batch
 * for this specific batch run is appended to the list. When batch run completes, the list is given to
 * {@link RecordStore#store(List)} instance in order to be stored permanently.
 * [DESIGN DECISION]: This class is not thread safe since it's intended to be used by a single queue consumer thread.
 */
class BatchRun<T> {
    private final String batchRunId;
    // [DESIGN DECISION]: ArrayList is chosen to store batches until the batch run is complete,
    // for the low complexity of adding and getting records (O(1)),
    private final ArrayList<T> records = new ArrayList<>();

    BatchRun(String batchRunId) {
        this.batchRunId = batchRunId;
    }

    void addBatch(T[] records) {
        this.records.addAll(Arrays.asList(records));
    }

    void clear() {
        records.clear();
    }

    public void dump() {
        System.out.println("-------------------" + batchRunId + " ------------------");
        records.forEach(System.out::println);
    }

    void appendTo(RecordStore<T> recordStore) {
        recordStore.store(records);
    }
}
