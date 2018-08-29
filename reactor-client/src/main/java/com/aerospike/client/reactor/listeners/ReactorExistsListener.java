package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsListener;
import reactor.core.publisher.MonoSink;

public class ReactorExistsListener implements ExistsListener {

	private final MonoSink<Key> sink;

	public ReactorExistsListener(MonoSink<Key> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key key, boolean exists) {
		if(exists){
			sink.success(key);
		} else {
			sink.success();
		}
	}
	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
