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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;

/**
 * Asynchronous result notifications for batch operate commands with variable operations.
 */
public interface BatchOperateListListener {
	/**
	 * This method is called when the command completes successfully.
	 *
	 * @param records		record instances, {@link com.aerospike.client.BatchRecord#record}
	 *						will be null if an error occurred for that key.
	 * @param status		true if all records returned success.
	 */
	public void onSuccess(List<BatchRecord> records, boolean status);

	/**
	 * This method is called when the command fails.
	 */
	public void onFailure(AerospikeException ae);
}
