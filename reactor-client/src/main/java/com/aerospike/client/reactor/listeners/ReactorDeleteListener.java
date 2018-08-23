package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.DeleteListener;
import reactor.core.publisher.MonoSink;

public class ReactorDeleteListener implements DeleteListener {

	private final MonoSink<Key> sink;

	public ReactorDeleteListener(MonoSink<Key> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key key, boolean existed) {
		if(existed){
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
