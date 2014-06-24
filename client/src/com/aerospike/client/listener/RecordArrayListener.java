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
package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Asynchronous result notifications for batch get commands.
 * The result is sent in a single array.
 */
public interface RecordArrayListener {
	/**
	 * This method is called when an asynchronous batch get command completes successfully.
	 * The returned record array is in positional order with the original key array order.
	 * 
	 * @param keys			unique record identifiers
	 * @param records		record instances, an instance will be null if the key is not found
	 */
	public void onSuccess(Key[] keys, Record[] records);
	
	/**
	 * This method is called when an asynchronous batch get command fails.
	 * 
	 * @param exception		error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
