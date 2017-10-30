/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

public final class NodeValidator {
	String name;
	List<Host> aliases;
	Host primaryHost;
	InetSocketAddress primaryAddress;
	Connection conn;
	int features;
	
	/**
	 * Add node(s) referenced by seed host aliases. In most cases, aliases reference
	 * a single node.  If round robin DNS configuration is used, the seed host may have 
	 * several aliases that reference different nodes in the cluster.
	 */
	public void seedNodes(Cluster cluster, Host host, HashMap<String,Node> nodesToAdd) throws Exception {
		setAliases(host);
		
		Exception exception = null;
		boolean found = false;
		
		for (Host alias : aliases) {			
			try {
				validateAlias(cluster, alias);
				found = true;
				
				if (! nodesToAdd.containsKey(name)) {
					// New node found.
					Node node = cluster.createNode(this);
					nodesToAdd.put(name, node);
				}
				else {
					// Node already referenced. Close connection.
					conn.close();
				}
			}
			catch (Exception e) {
				// Log and continue to next alias.
				if (Log.debugEnabled()) {
					Log.debug("Alias " + alias + " failed: " + Util.getErrorMessage(e));
				}
				
				if (exception == null) {
					exception = e;
				}
			}
		}
		
		if (! found) {
			// Exception can't be null here because setAliases() will throw exception 
			// if aliases length is zero.
			throw exception;
		}
	}
	
	/**
	 * Verify that a host alias references a valid node.
	 */
	public void validateNode(Cluster cluster, Host host) throws Exception {
		setAliases(host);
		
		Exception exception = null;
		
		for (Host alias : aliases) {			
			try {
				validateAlias(cluster, alias);
				return;
			}
			catch (Exception e) {
				// Log and continue to next alias.
				if (Log.debugEnabled()) {
					Log.debug("Alias " + alias + " failed: " + Util.getErrorMessage(e));
				}

				if (exception == null) {
					exception = e;
				}
			}
		}
		// Exception can't be null here because setAliases() will throw exception 
		// if aliases length is zero.
		throw exception;
	}

	private void setAliases(Host host) {
		InetAddress[] addresses;
		
		try {
			addresses = InetAddress.getAllByName(host.name);
		}
		catch (UnknownHostException uhe) {
			throw new AerospikeException.Connection("Invalid host: " + host);
		}
			
		if (addresses.length == 0) {
			throw new AerospikeException.Connection("Failed to find addresses for " + host);
		}
		
		// Add capacity for current address aliases plus IPV6 address and hostname.
		aliases = new ArrayList<Host>(addresses.length + 2);
		
		for (InetAddress address : addresses) {
			aliases.add(new Host(address.getHostAddress(), host.tlsName, host.port));
		}
	}
	
	private void validateAlias(Cluster cluster, Host alias) throws Exception {
		InetSocketAddress address = new InetSocketAddress(alias.name, alias.port);
		Connection conn = new Connection(cluster.tlsPolicy, alias.tlsName, address, cluster.getConnectionTimeout(), cluster.maxSocketIdleNanos, null);
		
		try {			
			if (cluster.user != null) {
				AdminCommand command = new AdminCommand(ThreadLocalData.getBuffer());
				command.authenticate(conn, cluster.user, cluster.password);
			}
			
			HashMap<String,String> map;
			boolean hasClusterName = cluster.clusterName != null && cluster.clusterName.length() > 0;
			
			if (hasClusterName) {
				map = Info.request(conn, "node", "partition-generation", "features", "cluster-name");			
			}
			else {
				map = Info.request(conn, "node", "partition-generation", "features");
			}
			
			String nodeName = map.get("node");
			
			if (nodeName == null) {
				throw new AerospikeException.InvalidNode();				
			}
			
			String genString = map.get("partition-generation");
			int gen;
			
			try {
				gen = Integer.parseInt(genString);
			}
			catch (Exception ex) {
				throw new AerospikeException.InvalidNode("Invalid partition-generation: " + genString);												
			}
						
			if (gen == -1) {
				throw new AerospikeException.InvalidNode("Node " + nodeName + ' ' + alias + " is not yet fully initialized");
			}

			if (hasClusterName) {
				String id = map.get("cluster-name");
				
				if (id == null || ! cluster.clusterName.equals(id)) {
					throw new AerospikeException.InvalidNode("Node " + nodeName + ' ' + alias + ' ' +
							" expected cluster name '" + cluster.clusterName + "' received '" + id + "'");
				}
			}
			
			this.name = nodeName;
			this.primaryHost = alias;
			this.primaryAddress = address;
			this.conn = conn;
			setFeatures(map);
		}
		catch (Exception e) {
			conn.close();
			throw e;
		}
	}
	
	private void setFeatures(HashMap<String,String> map) {
		try {
			String featuresString = map.get("features");
			int begin = 0;
			int end = 0;
			int len;
			
			while (end < featuresString.length()) {
				end = featuresString.indexOf(';', begin);
				
				if (end < 0) {
					end = featuresString.length();
				}
				len = end - begin;
				
				if (featuresString.regionMatches(begin, "geo", 0, len)) {
					this.features |= Node.HAS_GEO;
				}
				else if (featuresString.regionMatches(begin, "float", 0, len)) {
					this.features |= Node.HAS_DOUBLE;
				}
				else if (featuresString.regionMatches(begin, "batch-index", 0, len)) {
					this.features |= Node.HAS_BATCH_INDEX;
				}
				else if (featuresString.regionMatches(begin, "replicas", 0, len)) {
					this.features |= Node.HAS_REPLICAS;
				}
				else if (featuresString.regionMatches(begin, "replicas-all", 0, len)) {
					this.features |= Node.HAS_REPLICAS_ALL;
				}
				else if (featuresString.regionMatches(begin, "peers", 0, len)) {
					this.features |= Node.HAS_PEERS;
				}	        	
				begin = end + 1;
			}        
		}
		catch (Exception e) {
			// Unexpected exception. Use defaults.
		}
	}	
}
