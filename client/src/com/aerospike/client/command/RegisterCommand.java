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
package com.aerospike.client.command;

import gnu.crypto.util.Base64;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.Info.NameValueParser;
import com.aerospike.client.Language;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Environment;

public final class RegisterCommand {
	
	public static RegisterTask register(Cluster cluster, Policy policy, byte[] bytes, String serverPath, Language language) {	
		String content = Base64.encode(bytes, 0, bytes.length, false);
		
		StringBuilder sb = new StringBuilder(serverPath.length() + content.length() + 100);
		sb.append("udf-put:filename=");
		sb.append(serverPath);
		sb.append(";content=");
		sb.append(content);
		sb.append(";content-len=");
		sb.append(content.length());
		sb.append(";udf-type=");
		sb.append(language);
		sb.append(";");
		
		// Send UDF to one node. That node will distribute the UDF to other nodes.
		String command = sb.toString();
		Node node = cluster.getRandomNode();
		Connection conn = node.getConnection(policy.socketTimeout);
		
		try {			
			Info info = new Info(conn, command);
			NameValueParser parser = info.getNameValueParser();
			String error = null;
			String file = null;
			String line = null;
			String message = null;
			
			while (parser.next()) {
				String name = parser.getName();

				if (name.equals("error")) {
					error = parser.getValue();
				}
				else if (name.equals("file")) {
					file = parser.getValue();				
				}
				else if (name.equals("line")) {
					line = parser.getValue();				
				}
				else if (name.equals("message")) {
					message = parser.getStringBase64();					
				}
			}
			
			if (error != null) {			
				throw new AerospikeException("Registration failed: " + error + Environment.Newline +
					"File: " + file + Environment.Newline + 
					"Line: " + line + Environment.Newline +
					"Message: " + message
					);
			}
			
			node.putConnection(conn);
			return new RegisterTask(cluster, policy, serverPath);
		}
		catch (RuntimeException re) {
			node.closeConnection(conn);
			throw re;
		}
	}
}
