package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;
import reactor.core.publisher.MonoSink;

public class ReactorWriteListener implements WriteListener {

	private final MonoSink<Key> sink;

	public ReactorWriteListener(MonoSink<Key> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key key) {
		sink.success(key);
	}
	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}

}
