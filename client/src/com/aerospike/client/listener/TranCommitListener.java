/*
 * Copyright 2012-2024 Aerospike, Inc.
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
 * Asynchronous result notifications for multi-record transaction (MRT) commits.
 */
public interface TranCommitListener {
	/**
	 * This method is called when the records are verified and the commit succeeds.
	 */
	void onSuccess();

	/**
	 * This method is called when the commit fails.
	 *
	 * @param verifyRecords     record verify responses for each record read in the MRT
	 * @param rollRecords       record roll forward or backward responses for each record written in the MRT
	 * @param ae                exception indicating which part of the commit process failed
	 */
	void onFailure(BatchRecord[] verifyRecords, BatchRecord[] rollRecords, AerospikeException ae);
}
