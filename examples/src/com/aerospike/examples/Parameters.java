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
package com.aerospike.examples;

import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Configuration data.
 */
public class Parameters {
	String host;
	int port;
	String user;
	String password;
	String namespace;
	String set;
	AuthMode authMode;
	TlsPolicy tlsPolicy;
	WritePolicy writePolicy;
	Policy policy;
	EventLoopType eventLoopType = EventLoopType.DIRECT_NIO;
	int maxCommandsInProcess;
	int maxCommandsInQueue;

	protected Parameters(TlsPolicy policy, String host, int port, String user, String password, AuthMode authMode, String namespace, String set) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.authMode = authMode;
		this.namespace = namespace;
		this.set = set;
		this.tlsPolicy = policy;
	}

	@Override
	public String toString() {
		return "Parameters: host=" + host +
				" port=" + port +
				" ns=" + namespace +
				" set=" + set;
	}
}
