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

/**
 * Asynchronous result notifications for batch exists commands.
 * The result is sent in a single array.
 */
public interface ExistsArrayListener {
	/**
	 * This method is called when an asynchronous batch exists command completes successfully.
	 * The returned boolean array is in positional order with the original key array order.
	 * 
	 * @param keys				unique record identifiers
	 * @param exists			whether keys exists on server
	 */
	public void onSuccess(Key[] keys, boolean[] exists);
	
	/**
	 * This method is called when an asynchronous exists command fails.
	 * 
	 * @param exception			error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
