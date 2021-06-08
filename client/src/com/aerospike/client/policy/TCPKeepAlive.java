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
package com.aerospike.client.policy;

/**
 * TCP keep-alive policy. This configuration only referenced when using native Netty epoll library.
 */
public final class TCPKeepAlive {
	/**
	 * Idle time in seconds before TCP sends keep-alive packet.
	 * <p>
	 * Default: 59
	 */
	public final int idle;

	/**
	 * Wait time in seconds before retrying unacknowledged packet.
	 * <p>
	 * Default: 59
	 */
	public final int intvl;

	/**
	 * Maximum keep-alive packet attempts before invalidating the socket.
	 * <p>
	 * Default: 2
	 */
	public final int probes;

	/**
	 * Enable TCP keep-alive when using native Netty epoll library.
	 *
	 * @param idle 		idle time in seconds before TCP sends keep-alive packet
	 * @param intvl		wait time in seconds before retrying unacknowledged packet
	 * @param probes	maximum keep-alive packet attempts before invalidating the socket
	 */
	public TCPKeepAlive(int idle, int intvl, int probes) {
		this.idle = idle;
		this.intvl = intvl;
		this.probes = probes;
	}

	/**
	 * Copy TCP keep-alive policy from another keep-alive policy.
	 */
	public TCPKeepAlive(TCPKeepAlive other) {
		this.idle = other.idle;
		this.intvl = other.intvl;
		this.probes = other.probes;
	}

	/**
	 * Default constructor.
	 */
	public TCPKeepAlive() {
		this.idle = 59;  // Test socket 1 second before server.
		this.intvl = 59; // Retry socket 1 second before server.
		this.probes = 2;
	}
}
