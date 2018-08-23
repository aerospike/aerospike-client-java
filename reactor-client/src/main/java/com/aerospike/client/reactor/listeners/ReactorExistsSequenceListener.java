package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.reactor.dto.KeyExists;
import reactor.core.publisher.FluxSink;

public class ReactorExistsSequenceListener implements ExistsSequenceListener {

	private final FluxSink<KeyExists> sink;

	public ReactorExistsSequenceListener(FluxSink<KeyExists> sink) {
		this.sink = sink;
	}

	@Override
	public void onExists(Key key, boolean exists) {
		sink.next(new KeyExists(key, exists));
	}
	@Override
	public void onSuccess() {
		sink.complete();
	}
	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
