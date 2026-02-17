/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;

public class ServerInfo extends Example {

	public ServerInfo(Console console) {
		super(console);
	}

	/**
	 * Query server configuration, cluster status and namespace configuration.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		Node node = client.getNodes()[0];
		GetNamespaceConfig(node, params);
	}

	/**
	 * Query namespace configuration.
	 */
	private void GetNamespaceConfig(Node node, Parameters params) throws Exception {
		console.write("Namespace Configuration");
		String filter = "namespace/" + params.namespace;
		String tokens = Info.request(null, node, filter);

		if (tokens == null) {
			throw new Exception(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				params.host, params.port, params.namespace));
		}

		LogNameValueTokens(tokens);
	}

	private void LogNameValueTokens(String tokens) {
		String[] values = tokens.split(";");

		for (String value : values) {
			console.write(value);
		}
	}
}
