package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.listener.InfoListener;
import reactor.core.publisher.MonoSink;

import java.util.Map;

import static java.util.Collections.emptyMap;

public class ReactorInfoListener implements InfoListener {

    private final MonoSink<Map<String, String>> sink;

    public ReactorInfoListener(MonoSink<Map<String, String>> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(Map<String, String> map) {
        if(map == null){
            map = emptyMap();
        }
        sink.success(map);
    }

    @Override
    public void onFailure(AerospikeException ae) {
        sink.error(ae);
    }
}
