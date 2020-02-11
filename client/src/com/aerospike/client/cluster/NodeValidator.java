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
import com.aerospike.client.admin.AdminCommand.LoginCommand;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

public final class NodeValidator {
	String name;
	List<Host> aliases;
	Host primaryHost;
	InetSocketAddress primaryAddress;
	Connection primaryConn;
	byte[] sessionToken;
	long sessionExpiration;
	int features;

	/**
	 * Return first valid node referenced by seed host aliases. In most cases, aliases
	 * reference a single node.  If round robin DNS configuration is used, the seed host
	 * may have several addresses that reference different nodes in the cluster.
	 */
	public Node seedNode(Cluster cluster, Host host) throws Exception {
		InetAddress[] addresses = getAddresses(host);
		Exception exception = null;

		for (InetAddress address : addresses) {
			try {
				validateAddress(cluster, address, host.tlsName, host.port, true);

				// Only add address alias when not set by load balancer detection logic.
				if (this.aliases == null) {
					setAliases(address, host.tlsName, host.port);
				}
				return cluster.createNode(this);
			}
			catch (Exception e) {
				// Log exception and continue to next alias.
				if (Log.debugEnabled()) {
					Log.debug("Address " + address + ' ' + host.port + " failed: " + Util.getErrorMessage(e));
				}

				if (exception == null) {
					exception = e;
				}
			}
		}

		// Exception can't be null here because getAddresses() will throw exception
		// if aliases length is zero.
		throw exception;
	}

	/**
	 * Verify that a host alias references a valid node.
	 */
	public void validateNode(Cluster cluster, Host host) throws Exception {
		InetAddress[] addresses = getAddresses(host);
		Exception exception = null;

		for (InetAddress address : addresses) {
			try {
				validateAddress(cluster, address, host.tlsName, host.port, false);
				setAliases(address, host.tlsName, host.port);
				return;
			}
			catch (Exception e) {
				// Log exception and continue to next alias.
				if (Log.debugEnabled()) {
					Log.debug("Address " + address + ' ' + host.port + " failed: " + Util.getErrorMessage(e));
				}

				if (exception == null) {
					exception = e;
				}
			}
		}
		// Exception can't be null here because getAddresses() will throw exception
		// if aliases length is zero.
		throw exception;
	}

	private static InetAddress[] getAddresses(Host host) {
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
		return addresses;
	}

	private void validateAddress(Cluster cluster, InetAddress address, String tlsName, int port, boolean detectLoadBalancer)
		throws Exception {

		InetSocketAddress socketAddress = new InetSocketAddress(address, port);
		Connection conn = (cluster.tlsPolicy != null) ?
			new Connection(cluster.tlsPolicy, tlsName, socketAddress, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null) :
			new Connection(socketAddress, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null);

		try {
			if (cluster.user != null) {
				// Login
				LoginCommand admin = new LoginCommand(cluster, conn);
				sessionToken = admin.sessionToken;
				sessionExpiration = admin.sessionExpiration;

				if (cluster.tlsPolicy != null && cluster.tlsPolicy.forLoginOnly) {
					// Switch to using non-TLS socket.
					SwitchClear sc = new SwitchClear(cluster, conn, sessionToken);
					conn.close();
					address = sc.clearAddress;
					socketAddress = sc.clearSocketAddress;
					conn = sc.clearConn;

					// Disable load balancer detection since non-TLS address has already
					// been retrieved via service info command.
					detectLoadBalancer = false;
				}
			}

			List<String> commands = new ArrayList<String>(5);
			commands.add("node");
			commands.add("partition-generation");
			commands.add("features");

			boolean hasClusterName = cluster.clusterName != null && cluster.clusterName.length() > 0;

			if (hasClusterName) {
				commands.add("cluster-name");
			}

			String addressCommand = null;

			if (detectLoadBalancer) {
				if (address.isLoopbackAddress()) {
					// Disable load balancer detection for localhost.
					detectLoadBalancer = false;
				}
				else {
					// Seed may be load balancer with changing address. Determine real address.
					addressCommand = (cluster.tlsPolicy != null)?
						cluster.useServicesAlternate ? "service-tls-alt" : "service-tls-std" :
						cluster.useServicesAlternate ? "service-clear-alt" : "service-clear-std";

					commands.add(addressCommand);
				}
			}

			// Issue commands.
			HashMap<String,String> map = Info.request(conn, commands);

			// Node returned results.
			this.primaryHost = new Host(address.getHostAddress(), tlsName, port);
			this.primaryAddress = socketAddress;
			this.primaryConn = conn;

			validateNode(map);
			validatePartitionGeneration(map);
			setFeatures(map);

			if (hasClusterName) {
				validateClusterName(cluster, map);
			}

			if (addressCommand != null) {
				setAddress(cluster, map, addressCommand, tlsName);
			}
		}
		catch (Exception e) {
			conn.close();
			throw e;
		}
	}

	private void validateNode(HashMap<String,String> map) {
		this.name = map.get("node");

		if (this.name == null) {
			throw new AerospikeException.InvalidNode("Node name is null");
		}
	}

	private void validatePartitionGeneration(HashMap<String,String> map) {
		String genString = map.get("partition-generation");
		int gen;

		try {
			gen = Integer.parseInt(genString);
		}
		catch (Exception ex) {
			throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + " returned invalid partition-generation: " + genString);
		}

		if (gen == -1) {
			throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + " is not yet fully initialized");
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
				else if (featuresString.regionMatches(begin, "replicas", 0, len)) {
					this.features |= Node.HAS_REPLICAS;
				}
				else if (featuresString.regionMatches(begin, "peers", 0, len)) {
					this.features |= Node.HAS_PEERS;
				}
				else if (featuresString.regionMatches(begin, "cluster-stable", 0, len)) {
					this.features |= Node.HAS_CLUSTER_STABLE;
				}
				else if (featuresString.regionMatches(begin, "lut-now", 0, len)) {
					this.features |= Node.HAS_LUT_NOW;
				}
				else if (featuresString.regionMatches(begin, "truncate-namespace", 0, len)) {
					this.features |= Node.HAS_TRUNCATE_NS;
				}
				else if (featuresString.regionMatches(begin, "blob-bits", 0, len)) {
					this.features |= Node.HAS_BIT_OP;
				}
				else if (featuresString.regionMatches(begin, "sindex-exists", 0, len)) {
					this.features |= Node.HAS_INDEX_EXISTS;
				}
				else if (featuresString.regionMatches(begin, "pscans", 0, len)) {
					this.features |= Node.HAS_PARTITION_SCAN;
				}
				begin = end + 1;
			}
		}
		catch (Exception e) {
			// Unexpected exception. Use defaults.
		}
	}

	private void validateClusterName(Cluster cluster, HashMap<String,String> map) {
		String id = map.get("cluster-name");

		if (id == null || ! cluster.clusterName.equals(id)) {
			throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + ' ' +
					" expected cluster name '" + cluster.clusterName + "' received '" + id + "'");
		}
	}

	private void setAddress(Cluster cluster, HashMap<String,String> map, String addressCommand, String tlsName) {
		String result = map.get(addressCommand);

		if (result == null || result.length() == 0) {
			// Server does not support service level call (service-clear-std, ...).
			// Load balancer detection is not possible.
			return;
		}

		List<Host> hosts = Host.parseServiceHosts(result);
		Host h;

		// Search real hosts for seed.
		for (Host host : hosts) {
			h = host;

			if (cluster.ipMap != null) {
				String alt = cluster.ipMap.get(h.name);

				if (alt != null) {
					h = new Host(alt, h.port);
				}
			}

			if (h.equals(this.primaryHost)) {
				// Found seed which is not a load balancer.
				return;
			}
		}

		// Seed not found, so seed is probably a load balancer.
		// Find first valid real host.
		for (Host host : hosts) {
			try {
				h = host;

				if (cluster.ipMap != null) {
					String alt = cluster.ipMap.get(h.name);

					if (alt != null) {
						h = new Host(alt, h.port);
					}
				}

				InetAddress[] addresses = InetAddress.getAllByName(h.name);

				for (InetAddress address : addresses) {
					try {
						InetSocketAddress socketAddress = new InetSocketAddress(address, h.port);
						Connection conn = (cluster.tlsPolicy != null) ?
							new Connection(cluster.tlsPolicy, tlsName, socketAddress, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null) :
							new Connection(socketAddress, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null);

						try {
							if (cluster.user != null) {
								AdminCommand admin = new AdminCommand(ThreadLocalData.getBuffer());
								if (! admin.authenticate(cluster, conn, this.sessionToken)) {
									throw new AerospikeException("Authentication failed");
								}
							}

							// Authenticated connection.  Set real host.
							setAliases(address, tlsName, h.port);
							this.primaryHost = new Host(address.getHostAddress(), tlsName, h.port);
							this.primaryAddress = socketAddress;
							this.primaryConn.close();
							this.primaryConn = conn;
							return;
						}
						catch (Exception e) {
							conn.close();
						}
					}
					catch (Exception e) {
						// Try next address.
					}
				}
			}
			catch (Exception e) {
				// Try next host.
			}
		}

		// Failed to find a valid address. IP Address is probably internal on the cloud
		// because the server access-address is not configured.  Log warning and continue
		// with original seed.
		if (Log.infoEnabled()) {
			Log.info("Invalid address " + result + ". access-address is probably not configured on server.");
		}
	}

	private void setAliases(InetAddress address, String tlsName, int port) {
		// Add capacity for current address plus IPV6 address and hostname.
		this.aliases = new ArrayList<Host>(3);
		this.aliases.add(new Host(address.getHostAddress(), tlsName, port));
	}

	private static final class SwitchClear {
		private InetAddress clearAddress;
		private InetSocketAddress clearSocketAddress;
		private Connection clearConn;

		// Switch from TLS connection to non-TLS connection.
		private SwitchClear(Cluster cluster, Connection conn, byte[] sessionToken) throws Exception {
			// Obtain non-TLS addresses.
			String command = cluster.useServicesAlternate ? "service-clear-alt" : "service-clear-std";
			String result = Info.request(conn, command);
			List<Host> hosts = Host.parseServiceHosts(result);
			Host clearHost;

			// Find first valid non-TLS host.
			for (Host host : hosts) {
				try {
					clearHost = host;

					if (cluster.ipMap != null) {
						String alternativeHost = cluster.ipMap.get(clearHost.name);

						if (alternativeHost != null) {
							clearHost = new Host(alternativeHost, clearHost.port);
						}
					}

					InetAddress[] addresses = InetAddress.getAllByName(clearHost.name);

					for (InetAddress ia : addresses) {
						try {
							clearAddress = ia;
							clearSocketAddress = new InetSocketAddress(clearAddress, clearHost.port);
							clearConn = new Connection(clearSocketAddress, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null);

							try {
								AdminCommand admin = new AdminCommand(ThreadLocalData.getBuffer());
								if (! admin.authenticate(cluster, clearConn, sessionToken)) {
									throw new AerospikeException("Authentication failed");
								}
								return;  // Authenticated clear connection.
							}
							catch (Exception e) {
								clearConn.close();
							}
						}
						catch (Exception e) {
							// Try next address.
						}
					}
				}
				catch (Exception e) {
					// Try next host.
				}
			}
			throw new AerospikeException("Invalid non-TLS address: " + result);
		}
	}
}
