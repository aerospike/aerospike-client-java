/*
 * Citrusleaf Aerospike Client - Java Library
 *
 * Copyright 2009-2010 by Citrusleaf, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package net.citrusleaf;

import java.net.InetSocketAddress;
import java.util.HashMap;

import com.aerospike.client.Info;

/**
 * Legacy compatibility Layer. This class should only be used for legacy code.
 * Please use <code>com.aerospike.client.Info</code> for new code.
 * <p>
 * Access Citrusleaf's monitoring protocol - the "Info" protocol.
 * <p>
 * The info protocol is a name/value pair based system, where an individual
 * database server node is queried to determine its configuration and status.
 * The list of supported names can be found on the Citrusleaf Wiki under the TCP
 * wire protocol specification.
 * <p>
 * Citrusleaf info values are accessible via a node's host name and port, or
 * directly via its <code>InetSocketAddress</code>.
 */
public class CitrusleafInfo {

	//=================================================================
	// Public Functions
	//

	//-------------------------------------------------------
	// Get Info via Host Name and Port
	//-------------------------------------------------------

	/**
	 * Get all the default info from the specified database server node, using
	 * host name and port.
	 * 
	 * @param hostname			host name
	 * @param port				host port
	 * @return					info name/value pairs
	 */
	public static HashMap<String, String> get(String hostname, int port) {
		try {	
			return Info.request(hostname, port);
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * Get one info value by name from the specified database server node, using
	 * host name and port.
	 * 
	 * @param hostname			host name
	 * @param port				host port
	 * @param name				name of value to retrieve
	 * @return					info value
	 */
	public static String get(String hostname, int port, String name) {
		try {	
			return Info.request(hostname, port, name);
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * Get many info values by name from the specified database server node,
	 * using host name and port.
	 * 
	 * @param hostname			host name
	 * @param port				host port
	 * @param names				names of values to retrieve
	 * @return					info name/value pairs
	 */
	public static HashMap<String, String> get(String hostname, int port,
			String[] names) {
		try {	
			return Info.request(hostname, port, names);
		}
		catch (Exception e) {
			return null;
		}
	}

	//-------------------------------------------------------
	// Get Info via Socket Address
	//-------------------------------------------------------

	/**
	 * Get all the default info from the specified database server node.
	 * 
	 * @param isa				<code>InetSocketAddress</code> of server node
	 * @return					info name/value pairs
	 */
	public static HashMap<String, String> get(InetSocketAddress isa) {
		try {	
			return Info.request(isa);
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * Get one info value by name from the specified database server node.
	 * 
	 * @param isa				<code>InetSocketAddress</code> of server node
	 * @param name				name of value to retrieve
	 * @return					info value
	 */
	public static String get(InetSocketAddress isa, String name) {
		try {	
			return Info.request(isa, name);
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * Get many info values by name from the specified database server node.
	 * 
	 * @param isa				<code>InetSocketAddress</code> of server node
	 * @param names				names of values to retrieve
	 * @return					info name/value pairs
	 */
	public static HashMap<String, String> get(InetSocketAddress isa,
			String[] names) {		
		try {	
			return Info.request(isa, names);
		}
		catch (Exception e) {
			return null;
		}
	}
}
