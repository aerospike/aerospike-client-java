/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Record;

public class Batch extends Example {

	public Batch(Console console) {
		super(console);
	}

	/**
	 * Batch multiple gets in one call to the server.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		String keyPrefix = "batchkey";
		String valuePrefix = "batchvalue";
		String binName = params.getBinName("batchbin");  
		int size = 8;

		writeRecords(client, params, keyPrefix, binName, valuePrefix, size);
		batchExists(client, params, keyPrefix, size);
		batchReads(client, params, keyPrefix, binName, size);
		batchReadHeaders(client, params, keyPrefix, size);
	}

	/**
	 * Write records individually.
	 */
	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		String valuePrefix,
		int size
	) throws Exception {
		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, valuePrefix + i);
			
			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);
			
			client.put(params.writePolicy, key, bin);
		}
	}

	/**
	 * Check existence of records in one batch.
	 */
	private void batchExists (
		AerospikeClient client, 
		Parameters params,
		String keyPrefix,
		int size
	) throws Exception {
		// Batch into one call.
		Key[] keys = new Key[size];
		for (int i = 0; i < size; i++) {
			keys[i] = new Key(params.namespace, params.set, keyPrefix + (i + 1));
		}

		boolean[] existsArray = client.exists(null, keys);

		for (int i = 0; i < existsArray.length; i++) {
			Key key = keys[i];
			boolean exists = existsArray[i];
            console.info("Record: ns=%s set=%s key=%s exists=%s",
            	key.namespace, key.setName, key.userKey, exists);                        	
        }
    }

	/**
	 * Read records in one batch.
	 */
	private void batchReads (
		AerospikeClient client, 
		Parameters params,
		String keyPrefix,
		String binName,
		int size
	) throws Exception {
		// Batch gets into one call.
		Key[] keys = new Key[size];
		for (int i = 0; i < size; i++) {
			keys[i] = new Key(params.namespace, params.set, keyPrefix + (i + 1));
		}

		Record[] records = client.get(null, keys, binName);

		for (int i = 0; i < records.length; i++) {
			Key key = keys[i];
			Record record = records[i];
			Level level = Level.ERROR;
			Object value = null;
			
			if (record != null) {
				level = Level.INFO;
				value = record.getValue(binName);
			}
	        console.write(level, "Record: ns=%s set=%s key=%s bin=%s value=%s",
	            key.namespace, key.setName, key.userKey, binName, value);
        }
		
		if (records.length != size) {
        	console.error("Record size mismatch. Expected %d. Received %d.", size, records.length);
		}
    }
	
	/**
	 * Read record header data in one batch.
	 */
	private void batchReadHeaders (
		AerospikeClient client, 
		Parameters params,
		String keyPrefix,
		int size
	) throws Exception {
		// Batch gets into one call.
		Key[] keys = new Key[size];
		for (int i = 0; i < size; i++) {
			keys[i] = new Key(params.namespace, params.set, keyPrefix + (i + 1));
		}

		Record[] records = client.getHeader(null, keys);

		for (int i = 0; i < records.length; i++) {
			Key key = keys[i];
			Record record = records[i];
			Level level = Level.ERROR;
			int generation = 0;
			int expiration = 0;
			
			if (record != null && (record.generation > 0 || record.expiration > 0)) {
				level = Level.INFO;
				generation = record.generation;
				expiration = record.expiration;
			}
	        console.write(level, "Record: ns=%s set=%s key=%s generation=%d expiration=%d",
	            key.namespace, key.setName, key.userKey, generation, expiration);
        }
		
		if (records.length != size) {
        	console.error("Record size mismatch. Expected %d. Received %d.", size, records.length);
		}
    }
}
