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
import java.util.Arrays;
import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

public final class NodeValidator {
	String name;
	Host[] aliases;
	InetSocketAddress address;
	boolean useNewInfo = true;

	public NodeValidator(Host host, int timeoutMillis) throws AerospikeException {
		setAliases(host);
		setAddress(timeoutMillis);
	}
	
	private void setAliases(Host host) throws AerospikeException {	
		try {
			InetAddress[] addresses = InetAddress.getAllByName(host.name);
			int count = 0;
			aliases = new Host[addresses.length];
			
			for (InetAddress address : addresses) {
				aliases[count++] = new Host(address.getHostAddress(), host.port);
			}
		}
		catch (UnknownHostException uhe) {
			throw new AerospikeException.Connection("Invalid host: " + host);
		}
	}

	private void setAddress(int timeoutMillis) throws AerospikeException {
		for (Host alias : aliases) {
			try {
				InetSocketAddress address = new InetSocketAddress(alias.name, alias.port);
				Connection conn = new Connection(address, timeoutMillis);
				
				try {			
					HashMap<String,String> map = Info.request(conn, "node", "build");
					String nodeName = map.get("node");
					
					if (nodeName != null) {
						this.name = nodeName;
						this.address = address;
						
						// Check new info protocol support for >= 2.6.6 build
						String buildVersion = map.get("build");
						
						if (buildVersion != null) {
							try {
								String[] vNumber = buildVersion.split("\\.");
								int v1 = Integer.parseInt(vNumber[0]);
								int v2 = Integer.parseInt(vNumber[1]);
								int v3 = Integer.parseInt(vNumber[2]);
								
								this.useNewInfo = v1 > 2 || (v1 == 2 && (v2 > 6 || (v2 == 6 && v3 >= 6)));
							}
							catch (Exception e) {
								// Unexpected exception. Use default info protocol.
							}
						}
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
			}	
		}		
		throw new AerospikeException.Connection("Failed to connect to host aliases: " + Arrays.toString(aliases));
	}
}
