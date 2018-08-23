package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchListListener;
import reactor.core.publisher.MonoSink;

import java.util.List;

public class ReactorBatchListListener implements BatchListListener {

	private final MonoSink<List<BatchRead>> sink;

	public ReactorBatchListListener(MonoSink<List<BatchRead>> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(List<BatchRead> records) {
		sink.success(records);
	}

	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
