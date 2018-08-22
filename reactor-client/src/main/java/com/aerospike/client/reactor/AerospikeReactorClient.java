package com.aerospike.client.reactor;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.listener.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.dto.KeyExists;
import com.aerospike.client.reactor.dto.KeyObject;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Instantiate an <code>AerospikeReactorClient</code> object to access an Aerospike
 * database cluster and perform database operations reactively.
 * <p>
 * This client is just wrapper over AerospikeClient async methods that provides Reactor interface
 * <p>
 * This client is thread-safe. One client instance should be used per cluster.
 * Multiple threads should share this cluster instance.
 * <p>
 * Your application uses this class API to perform database operations such as
 * writing and reading records, and selecting sets of records. Write operations
 * include specialized functionality such as append/prepend and arithmetic
 * addition.
 * <p>
 * Each record may have multiple bins, unless the Aerospike server nodes are
 * configured as "single-bin". In "multi-bin" mode, partial records may be
 * written or read by specifying the relevant subset of bins.
 *
 * @author Sergii Karpenko
 */
public class AerospikeReactorClient implements IAerospikeReactorClient{

	private final AerospikeClient aerospikeClient;
	private final EventLoops eventLoops;

	public AerospikeReactorClient(AerospikeClient aerospikeClient, EventLoops eventLoops) {
		this.aerospikeClient = aerospikeClient;
		this.eventLoops = eventLoops;
	}

	@Override
	public void close(){
		aerospikeClient.close();
		eventLoops.close();
	}

	@Override
	public final Mono<KeyRecord> get(Key key) throws AerospikeException {
		return get(null, key);
	}

	@Override
	public final Mono<KeyRecord> get(Policy policy, Key key) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.get(eventLoops.next(), new RecordListener() {
					@Override
					public void onSuccess(Key key, Record record) {
						sink.success(new KeyRecord(key, record));
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key));
	}

	@Override
	public final Mono<KeysRecords> get(Key[] keys) throws AerospikeException {
		return get(null, keys);
	}

	@Override
	public final Mono<KeysRecords> get(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.get(eventLoops.next(), new RecordArrayListener() {
					@Override
					public void onSuccess(Key[] keys, Record[] records) {
						sink.success(new KeysRecords(keys, records));
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, keys));
	}

	@Override
	public final Mono<List<BatchRead>> get(List<BatchRead> records) throws AerospikeException {
		return get(null, records);
	}

	@Override
	public final Mono<List<BatchRead>> get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.get(eventLoops.next(), new BatchListListener() {

					@Override
					public void onSuccess(List<BatchRead> records) {
						sink.success(records);
					}

					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, records));
	}

	@Override
	public final Flux<BatchRead> getFlux(List<BatchRead> records) throws AerospikeException {
		return getFlux(null, records);
	}

	@Override
	public final Flux<BatchRead> getFlux(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		return Flux.create(sink -> aerospikeClient.get(eventLoops.next(), new BatchSequenceListener() {
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
				},
				policy, records));
	}

	@Override
	public final Flux<KeyRecord> getFlux(Key[] keys) throws AerospikeException {
		return getFlux(null, keys);
	}

	@Override
	public final Flux<KeyRecord> getFlux(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return Flux.create(sink -> aerospikeClient.get(eventLoops.next(), new RecordSequenceListener() {
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
				},
				policy, keys));
	}

	@Override
	public final Mono<KeyRecord> getHeader(Key key) throws AerospikeException {
		return getHeader(null, key);
	}

	@Override
	public final Mono<KeyRecord> getHeader(Policy policy, Key key) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.getHeader(eventLoops.next(), new RecordListener() {
					@Override
					public void onSuccess(Key key, Record record) {
						sink.success(new KeyRecord(key, record));
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key));
	}

	@Override
	public final Mono<KeysRecords> getHeaders(Key[] keys) throws AerospikeException {
		return getHeaders(null, keys);
	}

	@Override
	public final Mono<KeysRecords> getHeaders(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.getHeader(eventLoops.next(), new RecordArrayListener() {
					@Override
					public void onSuccess(Key[] keys, Record[] records) {
						sink.success(new KeysRecords(keys, records));
					}

					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, keys));
	}

	@Override
	public final Mono<Key> exists(Key key) throws AerospikeException {
		return exists(null, key);
	}

	@Override
	public final Mono<Key> exists(Policy policy, Key key) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.exists(eventLoops.next(), new ExistsListener() {
					@Override
					public void onSuccess(Key key, boolean exists) {
						if(exists){
							sink.success(key);
						} else {
							sink.success();
						}
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key));
	}

	@Override
	public final Mono<KeysExists> exists(Key[] keys) throws AerospikeException {
		return exists(null, keys);
	}

	@Override
	public final Mono<KeysExists> exists(BatchPolicy policy, Key[] keys) throws AerospikeException{
		return Mono.create(sink -> aerospikeClient.exists(eventLoops.next(), new ExistsArrayListener() {
					@Override
					public void onSuccess(Key[] keys, boolean[] exists) {
						sink.success(new KeysExists(keys, exists));
					}

					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, keys));
	}

	@Override
	public final Flux<KeyExists> existsFlux(Key[] keys) throws AerospikeException {
		return existsFlux(null, keys);
	}

	@Override
	public final Flux<KeyExists> existsFlux(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return Flux.create(sink -> aerospikeClient.exists(eventLoops.next(), new ExistsSequenceListener() {
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
				},
				policy, keys));
	}

	@Override
	public final Mono<Key> put(Key key, Bin... bins) throws AerospikeException {
		return put(null, key, bins);
	}

	@Override
	public final Mono<Key> put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.put(eventLoops.next(), new WriteListener() {
					@Override
					public void onSuccess(Key key) {
						sink.success(key);
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key, bins));
	}

	@Override
	public final Mono<Key> append(Key key, Bin... bins) throws AerospikeException {
		return append(null, key, bins);
	}

	@Override
	public final Mono<Key> append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.append(eventLoops.next(), new WriteListener() {
					@Override
					public void onSuccess(Key key) {
						sink.success(key);
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key, bins));
	}

	@Override
	public final Mono<Key> prepend(Key key, Bin... bins) throws AerospikeException {
		return prepend(null, key, bins);
	}

	@Override
	public final Mono<Key> prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.prepend(eventLoops.next(), new WriteListener() {
					@Override
					public void onSuccess(Key key) {
						sink.success(key);
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key, bins));
	}

	@Override
	public final Mono<Key> add(Key key, Bin... bins) throws AerospikeException {
		return add(null, key, bins);
	}

	@Override
	public final Mono<Key> add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.add(eventLoops.next(), new WriteListener() {
					@Override
					public void onSuccess(Key key) {
						sink.success(key);
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key, bins));
	}

	@Override
	public final Mono<Key> delete(Key key) throws AerospikeException {
		return delete(null, key);
	}

	@Override
	public final Mono<Key> delete(WritePolicy policy, Key key) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.delete(eventLoops.next(), new DeleteListener() {
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
				},
				policy, key));
	}

	@Override
	public final Mono<KeyRecord> operate(Key key, Operation... operations) throws AerospikeException {
		return operate(null, key, operations);
	}

	@Override
	public final Mono<KeyRecord> operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.operate(eventLoops.next(), new RecordListener() {
					@Override
					public void onSuccess(Key key, Record record) {
						sink.success(new KeyRecord(key, record));
					}
					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key, operations));
	}

	@Override
	public final Flux<KeyRecord> query(Statement statement) throws AerospikeException {
		return query(null, statement);
	}

	@Override
	public final Flux<KeyRecord> query(QueryPolicy policy, Statement statement) throws AerospikeException {
		return Flux.create(sink -> aerospikeClient.query(eventLoops.next(), new RecordSequenceListener() {
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
				},
				policy, statement));
	}

	@Override
	public final Flux<KeyRecord> scanAll(String namespace, String setName, String... binNames) throws AerospikeException {
		return scanAll(null, namespace, setName, binNames);
	}

	@Override
	public final Flux<KeyRecord> scanAll(ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException {
		return Flux.create(sink -> aerospikeClient.scanAll(eventLoops.next(), new RecordSequenceListener() {
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
				},
				policy, namespace, setName, binNames));
	}

	@Override
	public final Mono<KeyObject> execute(Key key, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return execute(null, key, packageName, functionName, functionArgs);
	}

	@Override
	public final Mono<KeyObject> execute(WritePolicy policy, Key key,
								   String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return Mono.create(sink -> aerospikeClient.execute(eventLoops.next(), new ExecuteListener() {
					@Override
					public void onSuccess(Key key, Object obj) {
						sink.success(new KeyObject(key, obj));
					}

					@Override
					public void onFailure(AerospikeException exception) {
						sink.error(exception);
					}
				},
				policy, key, packageName, functionName, functionArgs));
	}
}
