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
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class Replace extends Example {

	public Replace(Console console) {
		super(console);
	}

	/**
	 * Demonstrate writing bins with replace option. Replace will cause all record bins
	 * to be overwritten.  If an existing bin is not referenced in the replace command,
	 * the bin will be deleted.
	 * <p>
	 * The replace command has a performance advantage over the default put, because 
	 * the server does not have to read the existing record before overwriting it.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		runReplaceExample(client, params);
		runReplaceOnlyExample(client, params);
	}
	
	public void runReplaceExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "replacekey");
		Bin bin1 = new Bin("bin1", "value1");
		Bin bin2 = new Bin("bin2", "value2");
		Bin bin3 = new Bin("bin3", "value3");

		console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
			key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);
		
		client.put(params.writePolicy, key, bin1, bin2);

		console.info("Replace with: namespace=%s set=%s key=%s bin=%s value=%s",
			key.namespace, key.setName, key.userKey, bin3.name, bin3.value);

		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;	
		client.put(policy, key, bin3);
		
		console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

		Record record = client.get(params.policy, key);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}

		if (record.getValue(bin1.name) == null) {
			console.info(bin1.name + " was deleted as expected.");
		}
		else {
			console.error(bin1.name + " found when it should have been deleted.");
		}
		
		if (record.getValue(bin2.name) == null) {
			console.info(bin2.name + " was deleted as expected.");
		}
		else {
			console.error(bin2.name + " found when it should have been deleted.");
		}
		validateBin(key, bin3, record);
	}

	public void runReplaceOnlyExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "replaceonlykey");
		Bin bin = new Bin("bin", "value");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		console.info("Replace record requiring that it exists: namespace=%s set=%s key=%s",
			key.namespace, key.setName, key.userKey);

		try {
			WritePolicy policy = new WritePolicy();
			policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
			client.put(policy, key, bin);
			
			console.error("Failure. This command should have resulted in an error.");
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {				
				console.info("Success. Key not found error returned as expected.");
			}
			else {
				throw ae;
			}			
		}
	}
	
	private void validateBin(Key key, Bin bin, Record record) {
		Object received = record.getValue(bin.name);
		String expected = bin.value.toString();
		
		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s bin=%s value=%s generation=%d expiration=%d", 
				key.namespace, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}
}
