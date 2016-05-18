/*
 * Copyright 2012-2016 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

public final class NodeValidator {
	String name;
	Host[] aliases;
	InetSocketAddress address;
	Connection conn;
	boolean hasBatchIndex;
	boolean hasReplicasAll;
	boolean hasDouble;
	boolean hasGeo;
	
	/**
	 * Add node(s) referenced by seed host aliases. In most cases, aliases reference
	 * a single node.  If round robin DNS configuration is used, the seed host may have 
	 * several aliases that reference different nodes in the cluster.
	 */
	public void seedNodes(Cluster cluster, Host host, ArrayList<Node> list) throws Exception {
		setAliases(host);
		
		Exception exception = null;
		boolean found = false;
		
		for (Host alias : aliases) {			
			try {
				validateAlias(cluster, alias);
				found = true;
				
				if (! findNodeName(list, name)) {
					// New node found.
					Node node = cluster.createNode(this);
					cluster.addAliases(node);
					list.add(node);
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
		
		aliases = new Host[addresses.length];
		
		for (int i = 0; i < addresses.length; i++) {
			aliases[i] = new Host(addresses[i].getHostAddress(), host.port);
		}
	}
	
	private void validateAlias(Cluster cluster, Host alias) throws Exception {
		InetSocketAddress address = new InetSocketAddress(alias.name, alias.port);
		Connection conn = new Connection(address, cluster.getConnectionTimeout());
		
		try {			
			if (cluster.user != null) {
				AdminCommand command = new AdminCommand(ThreadLocalData.getBuffer());
				command.authenticate(conn, cluster.user, cluster.password);
			}
			HashMap<String,String> map = Info.request(conn, "node", "features");
			String nodeName = map.get("node");
			
			if (nodeName != null) {
				this.name = nodeName;
				this.address = address;
				this.conn = conn;
				setFeatures(map);
				return;
			}
			else {
				throw new AerospikeException.InvalidNode();
			}
		}
		catch (Exception e) {
			conn.close();
			throw e;
		}
	}
	
	private void setFeatures(HashMap<String,String> map) {
		try {
			String features = map.get("features");
			int begin = 0;
			int end = 0;
			int len;
			
			while (end < features.length() && !(this.hasGeo &&
												this.hasDouble &&
												this.hasBatchIndex &&
												this.hasReplicasAll)) {
				end = features.indexOf(';', begin);
				
				if (end < 0) {
					end = features.length();
				}
				len = end - begin;
				
				if (features.regionMatches(begin, "geo", 0, len)) {
					this.hasGeo = true;
				}

				if (features.regionMatches(begin, "float", 0, len)) {
					this.hasDouble = true;
				}

				if (features.regionMatches(begin, "batch-index", 0, len)) {
					this.hasBatchIndex = true;
				}
				
				if (features.regionMatches(begin, "replicas-all", 0, len)) {
					this.hasReplicasAll = true;
				}	        	
				begin = end + 1;
			}        
		}
		catch (Exception e) {
			// Unexpected exception. Use defaults.
		}
	}
	
	private final static boolean findNodeName(ArrayList<Node> list, String name) {
		for (Node node : list) {
			if (node.getName().equals(name)) {
				return true;
			}
		}
		return false;
	}
}
