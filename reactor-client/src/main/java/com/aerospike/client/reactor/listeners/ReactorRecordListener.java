package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import reactor.core.publisher.MonoSink;

public class ReactorRecordListener implements com.aerospike.client.listener.RecordListener{

	private final MonoSink<KeyRecord> sink;

	public ReactorRecordListener(MonoSink<KeyRecord> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key key, Record record) {
		sink.success(new KeyRecord(key, record));
	}
	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
