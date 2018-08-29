package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.reactor.dto.KeysExists;
import reactor.core.publisher.MonoSink;

public class ReactorExistsArrayListener implements ExistsArrayListener {

	private final MonoSink<KeysExists> sink;

	public ReactorExistsArrayListener(MonoSink<KeysExists> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key[] keys, boolean[] exists) {
		sink.success(new KeysExists(keys, exists));
	}

	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
