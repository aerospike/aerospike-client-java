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
import java.util.Arrays;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

public final class NodeValidator {
	String name;
	Host[] aliases;
	InetSocketAddress address;
	boolean useNewInfo;

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
					String nodeName = Info.request(conn, "node");
					String buildVersion = Info.request(conn, "build");
					if (nodeName != null) {
						this.name = nodeName;
						this.address = address;
						String[] vNumber = buildVersion.split("\\.");
						this.useNewInfo = Integer.parseInt(vNumber[0])>=3 || (Integer.parseInt(vNumber[0])>=2 && (Integer.parseInt(vNumber[1])>=6 && Integer.parseInt(vNumber[2])>=6 ));
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
