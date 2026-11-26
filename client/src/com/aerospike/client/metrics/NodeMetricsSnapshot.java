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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable metrics snapshot for one cluster node.
 */
public final class NodeMetricsSnapshot {
	/** Aerospike node name. */
	public final String nodeName;

	/** Node service address. */
	public final String nodeAddress;

	/** Node service port. */
	public final int nodePort;

	/**
	 * Java legacy synchronous connection-pool metrics.
	 */
	public final ConnectionMetricsSnapshot syncConnections;

	/**
	 * Java legacy asynchronous connection-pool metrics.
	 */
	public final ConnectionMetricsSnapshot asyncConnections;

	/** Current total open connections to this node. */
	public final long openConnections;

	/**
	 * Node-scoped connection latency histogram.
	 */
	public final LatencySnapshot connectionLatency;

	/**
	 * Cumulative connection-open failures for this node. Remains zero until collection
	 * is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long connectionOpenFailure;

	/**
	 * Cumulative TLS handshake failures for this node. Remains zero until collection
	 * is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long connectionTlsFailure;

	/**
	 * Cumulative authentication failures for this node. Remains zero until collection
	 * is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long connectionAuthFailure;

	/** Immutable namespace metrics for this node. */
	public final List<NamespaceMetricsSnapshot> namespaces;

	/**
	 * Initialize an immutable node metrics snapshot.
	 */
	public NodeMetricsSnapshot(
		String nodeName,
		String nodeAddress,
		int nodePort,
		ConnectionMetricsSnapshot syncConnections,
		ConnectionMetricsSnapshot asyncConnections,
		long openConnections,
		LatencySnapshot connectionLatency,
		long connectionOpenFailure,
		long connectionTlsFailure,
		long connectionAuthFailure,
		List<NamespaceMetricsSnapshot> namespaces
	) {
		this.nodeName = nodeName;
		this.nodeAddress = nodeAddress;
		this.nodePort = nodePort;
		this.syncConnections = syncConnections;
		this.asyncConnections = asyncConnections;
		this.openConnections = openConnections;
		this.connectionLatency = connectionLatency;
		this.connectionOpenFailure = connectionOpenFailure;
		this.connectionTlsFailure = connectionTlsFailure;
		this.connectionAuthFailure = connectionAuthFailure;
		this.namespaces = Collections.unmodifiableList(new ArrayList<>(namespaces));
	}

	@Override
	public String toString() {
		return "NodeMetricsSnapshot{"
			+ "nodeName='" + nodeName + '\''
			+ ", nodeAddress='" + nodeAddress + '\''
			+ ", nodePort=" + nodePort
			+ ", syncConnections=" + syncConnections
			+ ", asyncConnections=" + asyncConnections
			+ ", openConnections=" + openConnections
			+ ", connectionLatency=" + connectionLatency
			+ ", connectionOpenFailure=" + connectionOpenFailure
			+ ", connectionTlsFailure=" + connectionTlsFailure
			+ ", connectionAuthFailure=" + connectionAuthFailure
			+ ", namespaces=" + namespaces
			+ '}';
	}
}
