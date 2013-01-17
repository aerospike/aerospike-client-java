/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;

public final class NodeValidator {
	String name;
	List<Host> aliases;
	InetSocketAddress address;

	public NodeValidator(Host host, int timeoutMillis) throws AerospikeException {
		try {
			setAliases(host);
			setAddress(timeoutMillis);
		}
		catch (Exception e) {
			throw new AerospikeException.Connection("Failed to add " + host + " to cluster: " + e.getMessage());
		}
	}
	
	private void setAliases(Host host) throws UnknownHostException {
		aliases = new ArrayList<Host>();
		aliases.add(host);
		
		InetAddress[] addresses = InetAddress.getAllByName(host.name);
		
		for (InetAddress address : addresses) {
			if (! address.getHostAddress().equals(host.name)) {
				aliases.add(new Host(address.getHostAddress(), host.port));
			}
		}
	}

	private void setAddress(int timeoutMillis) throws Exception {
		for (Host alias : aliases) {
			try {
				InetSocketAddress address = new InetSocketAddress(alias.name, alias.port);
				Connection conn = new Connection(address, timeoutMillis);
				
				try {			
					String nodeName = Info.request(conn, "node");
					
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
			}	
		}
		throw new Exception();
	}
}
