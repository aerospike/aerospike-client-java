package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.reactor.dto.KeysRecords;
import reactor.core.publisher.MonoSink;

public class ReactorRecordArrayListener implements RecordArrayListener {

	private final MonoSink<KeysRecords> sink;

	public ReactorRecordArrayListener(MonoSink<KeysRecords> sink) {
		this.sink = sink;
	}

	@Override
	public void onSuccess(Key[] keys, Record[] records) {
		sink.success(new KeysRecords(keys, records));
	}

	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
