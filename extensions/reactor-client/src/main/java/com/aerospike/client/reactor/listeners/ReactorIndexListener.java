package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.AsyncIndexTask;
import com.aerospike.client.listener.IndexListener;
import reactor.core.publisher.MonoSink;

public class ReactorIndexListener implements IndexListener {

    private final MonoSink<AsyncIndexTask> sink;

    public ReactorIndexListener(MonoSink<AsyncIndexTask> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(AsyncIndexTask indexTask) {
        sink.success(indexTask);
    }

    @Override
    public void onFailure(AerospikeException ae) {
        sink.error(ae);
    }

}
