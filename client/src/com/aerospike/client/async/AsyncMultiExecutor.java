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
package com.aerospike.client.async;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;

public abstract class AsyncMultiExecutor {

	private final AtomicInteger completedCount = new AtomicInteger();
	protected int completedSize;
	private boolean failed;
	
	protected final void childSuccess() {
		int count = completedCount.incrementAndGet();
		
		if (!failed && count >= completedSize) {
			onSuccess();
		}
	}
	
	protected final void childFailure(AerospikeException ae) {
		failed = true;
		completedCount.incrementAndGet();
		onFailure(ae);
	}
	
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
