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
package com.aerospike.examples;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
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
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Node node = client.getNodes()[0];
		GetServerConfig(node, params);
		console.write("");
		GetNamespaceConfig(node, params);
	}

	/**
	 * Query server configuration and cluster status.
	 */
	private void GetServerConfig(Node node, Parameters params) throws Exception {
		console.write("Server Configuration");
		Map<String,String> map = Info.request(null, node);

		if (map == null) {
			throw new Exception(String.format("Failed to get server info: host=%s port=%d",
				params.host, params.port));
		}

		for (Map.Entry<String,String> entry : map.entrySet()) {
			String key = entry.getKey();
			
			if (key.equals("statistics") || key.equals("query-stat")) {
				LogNameValueTokens(entry.getValue());
			}
			else {
				console.write(key + '=' + entry.getValue());
			}
		}
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
