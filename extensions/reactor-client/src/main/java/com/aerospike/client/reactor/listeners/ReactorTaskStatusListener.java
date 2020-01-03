package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.listener.TaskStatusListener;
import reactor.core.publisher.MonoSink;

public class ReactorTaskStatusListener implements TaskStatusListener {

    private final MonoSink<Integer> sink;

    public ReactorTaskStatusListener(MonoSink<Integer> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(int status) {
        sink.success(status);
    }

    @Override
    public void onFailure(AerospikeException ae) {
        sink.error(ae);
    }

}
