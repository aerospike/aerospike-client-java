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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CounterStore {

	Current write = new Current();
	Current read = new Current();
	Current transaction = new Current();
	Current mrtUnitOfWork = new Current();
	Current mrtCommit = new Current();
	Current mrtAbort = new Current();

	boolean showMicroSeconds = false;

	AtomicLong periodBegin = new AtomicLong();
	AtomicInteger readNotFound = new AtomicInteger();
	AtomicInteger valueMismatchCnt = new AtomicInteger();
	AtomicInteger loadValuesFinishedTasks = new AtomicInteger();
	AtomicBoolean loadValuesFinished = new AtomicBoolean(false);

	public void setOpenTelemetry(OpenTelemetry openTelemetry) {
		// Set the Open Tel instance for each latency type we are measuring
		if(this.write.latency != null) {
			this.write.latency.setOpenTelemetry(openTelemetry);
		}
		if(this.read.latency != null) {
			this.read.latency.setOpenTelemetry(openTelemetry);
		}
		if(this.transaction.latency != null) {
			this.transaction.latency.setOpenTelemetry(openTelemetry);
		}
		if(this.mrtUnitOfWork.latency != null) {
			this.mrtUnitOfWork.latency.setOpenTelemetry(openTelemetry);
		}
		if(this.mrtCommit.latency != null) {
			this.mrtCommit.latency.setOpenTelemetry(openTelemetry);
		}
		if(this.mrtAbort.latency != null) {
			this.mrtAbort.latency.setOpenTelemetry(openTelemetry);
		}
		this.write.openTelemetry = openTelemetry;
		this.read.openTelemetry = openTelemetry;
		this.transaction.openTelemetry = openTelemetry;
		this.mrtUnitOfWork.openTelemetry = openTelemetry;
		this.mrtCommit.openTelemetry = openTelemetry;
		this.mrtAbort.openTelemetry = openTelemetry;
	}

	public static class Current {
		AtomicInteger count = new AtomicInteger();
		AtomicInteger timeouts = new AtomicInteger();
		AtomicInteger errors = new AtomicInteger();
		AtomicInteger inDoubt = new AtomicInteger();
		AtomicInteger blocked = new AtomicInteger();
		AtomicInteger min = new AtomicInteger(-1);
		AtomicInteger max = new AtomicInteger(-1);
		LatencyManager latency;
		OpenTelemetry openTelemetry = null;

		public void addExceptionOTel(Exception e, LatencyTypes latencyType) {
			if(openTelemetry != null) {
				openTelemetry.addException(e, latencyType);
			}
		}
		public void addExceptionOTel(String exceptionType, String exception_subtype, String message, LatencyTypes latencyType) {
			if(openTelemetry != null) {
				openTelemetry.addException(exceptionType, exception_subtype, message, latencyType);
			}
		}
		public void incrTransCountOTel(LatencyTypes type) {
			if(openTelemetry != null) {
				openTelemetry.incrTransCounter(type);
			}
		}
		public void recordElapsedTimeOTel(LatencyTypes type, long elapsedNanos) {
			if(openTelemetry != null) {
				openTelemetry.recordElapsedTime(type, elapsedNanos);
			}
		}
	}
}
