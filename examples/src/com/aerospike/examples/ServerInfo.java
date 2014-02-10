/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.examples;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;

public class ServerInfo extends Example {

	public ServerInfo(Console console) {
		super(console);
	}

	/**
	 * Query server configuration, cluster status and namespace configuration.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		GetServerConfig(params);
		console.write("");
		GetNamespaceConfig(params);
	}

	/**
	 * Query server configuration and cluster status.
	 */
	private void GetServerConfig(Parameters params) throws Exception {
		console.write("Server Configuration");
		Map<String,String> map = Info.request(params.host, params.port);

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
	private void GetNamespaceConfig(Parameters params) throws Exception {
		console.write("Namespace Configuration");
		String filter = "namespace/" + params.namespace;
		String tokens = Info.request(params.host, params.port, filter);

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
