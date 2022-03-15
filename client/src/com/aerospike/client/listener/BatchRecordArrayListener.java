/*
 * Copyright 2012-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
import com.aerospike.client.BatchRecord;

/**
 * Asynchronous result notifications for batch operate commands.
 */
public interface BatchRecordArrayListener {
	/**
	 * This method is called when the command completes successfully.
	 * The returned record array is in positional order with the original key array order.
	 *
	 * @param records		record instances, always populated.
	 * @param status		true if all records returned success.
	 */
	public void onSuccess(BatchRecord[] records, boolean status);

	/**
	 * This method is called when one or more keys fail.
	 *
	 * @param records		record instances, always populated. {@link com.aerospike.client.BatchRecord#resultCode}
	 * 						indicates if an error occurred for each record instance.
	 * @param ae			error that occurred
	 */
	public void onFailure(BatchRecord[] records, AerospikeException ae);
}
