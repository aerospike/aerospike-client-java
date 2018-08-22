/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.reactor.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.TlsPolicy;

import java.util.Map;

public class Args {

	public String host = "127.0.0.1";
	public int port = 3000;
	public AuthMode authMode = AuthMode.INTERNAL;
	public String user;
	public String password;
	public String namespace = "test";
	public String set = "test";
	public TlsPolicy tlsPolicy;
	public EventLoopType eventLoopType;
	public boolean hasUdf;
	public boolean hasMap;
	public boolean singleBin;
	
	public Args() {
	}

	public Args setEventLoopType(EventLoopType eventLoopType){
		this.eventLoopType = eventLoopType;
		return this;
	}

	@Override
	public String toString() {
		return "eventLoopType="+eventLoopType;
	}

	/**
	 * Some database calls need to know how the server is configured.
	 */
	public void setServerSpecific(AerospikeClient client) {
		Node node = client.getNodes()[0];
		String featuresFilter = "features";
		String namespaceFilter = "namespace/" + namespace;
		Map<String,String> tokens = Info.request(null, node, featuresFilter, namespaceFilter);

		String features = tokens.get(featuresFilter);
		hasUdf = false;
		hasMap = false;
		
		if (features != null) {
			String[] list = features.split(";");
			
			for (String s : list) {
				if (s.equals("udf")) {
					hasUdf = true;
					break;
				}
				else if (s.equals("cdt-map")) {
					hasMap = true;
					break;
				}
			}
		}
		
		String namespaceTokens = tokens.get(namespaceFilter);
		
		if (namespaceTokens == null) {
			throw new AerospikeException(String.format(
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
	
	public String getBinName(String name) {
		// Single bin servers don't need a bin name.
		return singleBin ? "" : name;
	}
	
}
