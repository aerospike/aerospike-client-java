/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.metrics;

/**
 * Immutable metrics for one namespace on one node.
 */
public final class NamespaceMetricsSnapshot {
	/** Namespace name. */
	public final String namespace;

	/** Cumulative errors not represented by a more specific namespace counter. */
	public final long errors;

	/** Cumulative client-side command timeouts for this namespace. */
	public final long clientTimeouts;

	/**
	 * Cumulative server-side command timeouts for this namespace. Remains zero until
	 * collection distinguishes timeout origin.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long serverTimeouts;

	/** Cumulative key-busy errors for this namespace. */
	public final long keyBusy;

	/**
	 * Cumulative record-too-big errors for this namespace. Remains zero until collection
	 * is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long recordTooBig;

	/**
	 * Cumulative device-overload errors for this namespace. Remains zero until collection
	 * is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long deviceOverload;

	/** Cumulative payload bytes sent to this namespace. */
	public final long bytesIn;

	/** Cumulative payload bytes received from this namespace. */
	public final long bytesOut;

	/** Read latency histogram for this namespace, or null when unavailable. */
	public final LatencySnapshot readLatency;

	/** Write latency histogram for this namespace, or null when unavailable. */
	public final LatencySnapshot writeLatency;

	/** Batch latency histogram for this namespace, or null when unavailable. */
	public final LatencySnapshot batchLatency;

	/** Query latency histogram for this namespace, or null when unavailable. */
	public final LatencySnapshot queryLatency;

	/**
	 * Initialize an immutable namespace metrics snapshot.
	 */
	public NamespaceMetricsSnapshot(
		String namespace,
		long errors,
		long clientTimeouts,
		long serverTimeouts,
		long keyBusy,
		long recordTooBig,
		long deviceOverload,
		long bytesIn,
		long bytesOut,
		LatencySnapshot readLatency,
		LatencySnapshot writeLatency,
		LatencySnapshot batchLatency,
		LatencySnapshot queryLatency
	) {
		this.namespace = namespace;
		this.errors = errors;
		this.clientTimeouts = clientTimeouts;
		this.serverTimeouts = serverTimeouts;
		this.keyBusy = keyBusy;
		this.recordTooBig = recordTooBig;
		this.deviceOverload = deviceOverload;
		this.bytesIn = bytesIn;
		this.bytesOut = bytesOut;
		this.readLatency = readLatency;
		this.writeLatency = writeLatency;
		this.batchLatency = batchLatency;
		this.queryLatency = queryLatency;
	}

	@Override
	public String toString() {
		return "NamespaceMetricsSnapshot{"
			+ "namespace='" + namespace + '\''
			+ ", errors=" + errors
			+ ", clientTimeouts=" + clientTimeouts
			+ ", serverTimeouts=" + serverTimeouts
			+ ", keyBusy=" + keyBusy
			+ ", recordTooBig=" + recordTooBig
			+ ", deviceOverload=" + deviceOverload
			+ ", bytesIn=" + bytesIn
			+ ", bytesOut=" + bytesOut
			+ ", readLatency=" + readLatency
			+ ", writeLatency=" + writeLatency
			+ ", batchLatency=" + batchLatency
			+ ", queryLatency=" + queryLatency
			+ '}';
	}
}
