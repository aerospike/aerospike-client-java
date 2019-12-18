package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;

public interface TaskStatusListener {

    /**
     * This method is called when an asynchronous command completes successfully.
     *
     * @param status
     */
    void onSuccess(int status);

    /**
     * This method is called when an asynchronous command fails.
     *
     * @param exception			error that occurred
     */
    public void onFailure(AerospikeException exception);

}
