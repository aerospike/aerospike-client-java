package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.AsyncIndexTask;

public interface IndexListener {

    /**
     * This method is called when an asynchronous command completes successfully.
     *
     * @param indexTask
     */
    void onSuccess(AsyncIndexTask indexTask);

    /**
     * This method is called when an asynchronous command fails.
     *
     * @param exception			error that occurred
     */
    public void onFailure(AerospikeException exception);

}
