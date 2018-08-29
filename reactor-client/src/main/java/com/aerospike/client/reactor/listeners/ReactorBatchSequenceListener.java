package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchSequenceListener;
import reactor.core.publisher.FluxSink;

public class ReactorBatchSequenceListener implements BatchSequenceListener {

	private final FluxSink<BatchRead> sink;

	public ReactorBatchSequenceListener(FluxSink<BatchRead> sink) {
		this.sink = sink;
	}

	@Override
	public void onRecord(BatchRead record) {
		sink.next(record);
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
