/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

/**
 * Server node address as advertised by the cluster.
 */
public final class Endpoint {
	/**
	 * Info command list the endpoint was returned by.
	 */
	public enum SourceList {
		/**
		 * Standard list: peers-clear-std, peers-tls-std, service-clear-std, service-tls-std.
		 */
		STANDARD,

		/**
		 * Alternate list: peers-clear-alt, peers-tls-alt, service-clear-alt, service-tls-alt.
		 * Used when {@link com.aerospike.client.policy.ClientPolicy#useServicesAlternate} is true.
		 */
		ALTERNATE
	}

	/**
	 * Name of the node that advertised the address.
	 */
	public final String nodeName;

	/**
	 * TLS certificate name of the node. May be null.
	 */
	public final String tlsName;

	/**
	 * Advertised host name or IP address.
	 */
	public final String host;

	/**
	 * Advertised port.
	 */
	public final int port;

	/**
	 * Info command list the address came from.
	 */
	public final SourceList sourceList;

	/**
	 * Initialize advertised endpoint.
	 */
	public Endpoint(String nodeName, String tlsName, String host, int port, SourceList sourceList) {
		this.nodeName = nodeName;
		this.tlsName = tlsName;
		this.host = host;
		this.port = port;
		this.sourceList = sourceList;
	}

	@Override
	public String toString() {
		return nodeName + ' ' + host + ':' + port + ' ' + sourceList;
	}
}
