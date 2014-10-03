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
package com.aerospike.client.cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.command.AdminCommand;
import com.aerospike.client.util.Util;

public final class NodeValidator {
	String name;
	Host[] aliases;
	InetSocketAddress address;

	public NodeValidator(Cluster cluster, Host host) throws Exception {
		try {
			InetAddress[] addresses = InetAddress.getAllByName(host.name);
			aliases = new Host[addresses.length];
			
			for (int i = 0; i < addresses.length; i++) {
				aliases[i] = new Host(addresses[i].getHostAddress(), host.port);
			}
		}
		catch (UnknownHostException uhe) {
			throw new AerospikeException.Connection("Invalid host: " + host);
		}

		Exception exception = null;
		
		for (int i = 0; i < aliases.length; i++) {
			Host alias = aliases[i];
			
			try {
				InetSocketAddress address = new InetSocketAddress(alias.name, alias.port);
				Connection conn = new Connection(address, cluster.getConnectionTimeout());
				
				try {			
					if (cluster.user != null) {
						AdminCommand command = new AdminCommand();
						command.authenticate(conn, cluster.user, cluster.password);
					}
					HashMap<String,String> map = Info.request(conn, "node", "build");
					String nodeName = map.get("node");
					
					if (nodeName != null) {
						this.name = nodeName;
						this.address = address;						
						return;
					}
				}
				finally {
					conn.close();
				}
			}
			catch (Exception e) {
				// Try next address.
				if (Log.debugEnabled()) {
					Log.debug("Alias " + alias + " failed: " + Util.getErrorMessage(e));
				}

				if (exception == null)
				{
					exception = e;
				}
			}	
		}		

		if (exception == null)
		{
			throw new AerospikeException.Connection("Failed to find addresses for " + host);
		}
		throw exception;
	}
}
