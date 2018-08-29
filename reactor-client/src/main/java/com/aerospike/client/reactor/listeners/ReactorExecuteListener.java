package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.reactor.dto.KeyObject;
import reactor.core.publisher.MonoSink;

public class ReactorExecuteListener implements ExecuteListener {

	private final MonoSink<KeyObject> sink;

	public ReactorExecuteListener(MonoSink<KeyObject> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key key, Object obj) {
		sink.success(new KeyObject(key, obj));
	}

	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
