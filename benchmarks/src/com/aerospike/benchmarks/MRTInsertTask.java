/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.benchmarks;

import com.aerospike.client.*;

public abstract class MRTInsertTask extends MRTTask {

	public MRTInsertTask(Arguments args, CounterStore counters) {
		super(args, counters);
	}

	protected void writeFailure(AerospikeException ae) {

		if(ae.getInDoubt()) {
			counters.write.inDoubt.getAndIncrement();
		}
		else if (ae.getResultCode() == ResultCode.TIMEOUT) {
			counters.write.timeouts.getAndIncrement();
		} else {
			counters.write.errors.getAndIncrement();

			if (args.debug) {
				ae.printStackTrace();
			}
		}
		counters.write.addExceptionOTel(ae, LatencyTypes.WRITE);
	}

	protected void writeFailure(Exception e) {
		counters.write.errors.getAndIncrement();

		if (args.debug) {
			e.printStackTrace();
		}
		counters.write.addExceptionOTel(e, LatencyTypes.WRITE);
	}
}