/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.cluster.Node;
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
	boolean singleBin;
	boolean hasGeo;
	boolean hasUdf;
	boolean hasCDTList;
	boolean hasCDTMap;

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

	/**
	 * Some database calls need to know how the server is configured.
	 */
	protected void setServerSpecific(AerospikeClient client) throws Exception {
		Node node = client.getNodes()[0];
		String featuresFilter = "features";
		String namespaceFilter = "namespace/" + namespace;
		Map<String,String> tokens = Info.request(null, node, featuresFilter, namespaceFilter);

		String features = tokens.get(featuresFilter);
		hasGeo = false;
		hasUdf = false;
		hasCDTList = false;
		hasCDTMap = false;

		if (features != null) {
			String[] list = features.split(";");

			for (String s : list) {
				if (s.equals("geo")) {
					hasGeo = true;
				}
				else if (s.equals("udf")) {
					hasUdf = true;
				}
				else if (s.equals("cdt-list")) {
					hasCDTList = true;
				}
				else if (s.equals("cdt-map")) {
					hasCDTMap = true;
				}
			}
		}

		String namespaceTokens = tokens.get(namespaceFilter);

		if (namespaceTokens == null) {
			throw new Exception(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				host, port, namespace));
		}

		singleBin = parseBoolean(namespaceTokens, "single-bin");
	}

	private static boolean parseBoolean(String namespaceTokens, String name) {
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			return false;
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}

		String value = namespaceTokens.substring(begin, end);
		return Boolean.parseBoolean(value);
	}

	@Override
	public String toString() {
		return "Parameters: host=" + host +
				" port=" + port +
				" ns=" + namespace +
				" set=" + set +
				" single-bin=" + singleBin;
	}

	public String getBinName(String name)
	{
		// Single bin servers don't need a bin name.
		return singleBin ? "" : name;
	}
}
