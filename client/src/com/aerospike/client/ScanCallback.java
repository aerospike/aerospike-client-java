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
package com.aerospike.client;

/**
 * An object implementing this interface is passed in <code>scan()</code> calls, so the caller can
 * be notified with scan results.
 */
public interface ScanCallback {
	/**
	 * This method will be called for each record returned from a scan. The user may throw a 
	 * {@link com.aerospike.client.AerospikeException.ScanTerminated AerospikeException.ScanTerminated} 
	 * exception if the scan should be aborted.  If any exception is thrown, parallel scan threads
	 * to other nodes will also be terminated and the exception will be propagated back through the
	 * initiating scan call.
	 * <p>
	 * Multiple threads will likely be calling scanCallback in parallel.  Therefore, your scanCallback
	 * implementation should be thread safe.
	 * 
	 * @param key					unique record identifier
	 * @param record				container for bins and record meta-data
	 * @throws AerospikeException	if error occurs or scan should be terminated.
	 */
	public void scanCallback(Key key, Record record) throws AerospikeException;
}
