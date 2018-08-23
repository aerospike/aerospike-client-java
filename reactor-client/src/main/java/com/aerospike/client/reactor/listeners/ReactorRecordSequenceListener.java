package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import reactor.core.publisher.FluxSink;

public class ReactorRecordSequenceListener implements RecordSequenceListener {

	private final FluxSink<KeyRecord> sink;

	public ReactorRecordSequenceListener(FluxSink<KeyRecord> sink) {
		this.sink = sink;
	}

	@Override
	public void onRecord(Key key, Record record) throws AerospikeException {
		sink.next(new KeyRecord(key, record));
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
